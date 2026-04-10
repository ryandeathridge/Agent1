"""Classify categories function - deterministic category classification."""
import logging
import json
import azure.functions as func
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np


def main(req: func.HttpRequest) -> func.HttpResponse:
    """Classify categories endpoint."""
    logging.info('Classify categories function triggered')
    
    try:
        req_body = req.get_json()
    except ValueError:
        return func.HttpResponse(
            json.dumps({"error": "Invalid JSON in request body"}),
            status_code=400,
            mimetype="application/json"
        )
    
    records = req_body.get('records', [])
    taxonomy = req_body.get('taxonomy', {})
    examples = req_body.get('examples', [])
    
    if not records or not taxonomy:
        return func.HttpResponse(
            json.dumps({"error": "records and taxonomy are required"}),
            status_code=400,
            mimetype="application/json"
        )
    
    try:
        keyword_map = _build_keyword_map(taxonomy)
        
        supplier_map = _build_supplier_map()
        
        example_vectorizer = None
        example_vectors = None
        example_categories = []
        
        if examples:
            example_texts = [ex.get('description', '') for ex in examples]
            example_categories = [
                {
                    'l1': ex.get('l1'),
                    'l2': ex.get('l2'),
                    'l3': ex.get('l3')
                }
                for ex in examples
            ]
            
            if example_texts:
                example_vectorizer = TfidfVectorizer(max_features=500, ngram_range=(1, 2))
                example_vectors = example_vectorizer.fit_transform(example_texts)
        
        classifications = []
        auto_classified = 0
        needs_llm = 0
        
        for record in records:
            record_id = record.get('record_id', '')
            description = record.get('description', '').lower()
            supplier_name = record.get('supplier_name', '').lower()
            
            signals = []
            
            keyword_match = _match_keywords(description, keyword_map)
            if keyword_match:
                signals.append(('keyword', keyword_match))
            
            supplier_match = _match_supplier(supplier_name, supplier_map)
            if supplier_match:
                signals.append(('supplier', supplier_match))
            
            example_match = None
            if example_vectorizer and example_vectors is not None:
                example_match = _match_examples(
                    description, 
                    example_vectorizer, 
                    example_vectors, 
                    example_categories
                )
                if example_match:
                    signals.append(('example', example_match))
            
            category, confidence = _resolve_signals(signals, taxonomy)
            
            if confidence >= 0.80:
                classifications.append({
                    'record_id': record_id,
                    'category_l1': category.get('l1'),
                    'category_l2': category.get('l2'),
                    'category_l3': category.get('l3'),
                    'confidence': confidence,
                    'needs_llm': False
                })
                auto_classified += 1
            else:
                best_guess = None
                if signals:
                    best_guess = f"{signals[0][1].get('l1')} > {signals[0][1].get('l2')} > {signals[0][1].get('l3')}"
                
                classifications.append({
                    'record_id': record_id,
                    'category_l1': None,
                    'category_l2': None,
                    'category_l3': None,
                    'confidence': confidence,
                    'needs_llm': True,
                    'best_guess': best_guess
                })
                needs_llm += 1
        
        return func.HttpResponse(
            json.dumps({
                "classifications": classifications,
                "stats": {
                    "auto_classified": auto_classified,
                    "needs_llm": needs_llm,
                    "total": len(records)
                }
            }),
            status_code=200,
            mimetype="application/json"
        )
        
    except Exception as e:
        logging.error(f"Error classifying categories: {str(e)}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500,
            mimetype="application/json"
        )


def _build_keyword_map(taxonomy: dict) -> dict:
    """Build a map of keywords to categories."""
    keyword_map = {}
    
    for l1, l2_dict in taxonomy.items():
        for l2, l3_list in l2_dict.items():
            for l3 in l3_list:
                keywords = l3.lower().split()
                for keyword in keywords:
                    if len(keyword) > 3:
                        if keyword not in keyword_map:
                            keyword_map[keyword] = []
                        keyword_map[keyword].append({'l1': l1, 'l2': l2, 'l3': l3})
    
    return keyword_map


def _build_supplier_map() -> dict:
    """Build a map of suppliers to typical categories."""
    return {
        'caterpillar': {'l1': 'Equipment & Parts', 'l2': 'Mobile Equipment', 'l3': 'Haul truck parts'},
        'komatsu': {'l1': 'Equipment & Parts', 'l2': 'Mobile Equipment', 'l3': 'Excavator parts'},
        'orica': {'l1': 'Raw Materials & Consumables', 'l2': 'Chemical Reagents', 'l3': 'Explosives'},
        'shell': {'l1': 'Energy & Fuel', 'l2': 'Diesel', 'l3': 'Bulk diesel'},
        'bp': {'l1': 'Energy & Fuel', 'l2': 'Diesel', 'l3': 'Bulk diesel'},
    }


def _match_keywords(description: str, keyword_map: dict) -> dict:
    """Match description against keyword map."""
    matches = []
    
    words = description.split()
    for word in words:
        if word in keyword_map:
            matches.extend(keyword_map[word])
    
    if not matches:
        return None
    
    category_counts = {}
    for match in matches:
        key = (match['l1'], match['l2'], match['l3'])
        category_counts[key] = category_counts.get(key, 0) + 1
    
    best_match = max(category_counts.items(), key=lambda x: x[1])
    return {'l1': best_match[0][0], 'l2': best_match[0][1], 'l3': best_match[0][2]}


def _match_supplier(supplier_name: str, supplier_map: dict) -> dict:
    """Match supplier name to typical category."""
    for supplier_keyword, category in supplier_map.items():
        if supplier_keyword in supplier_name:
            return category
    return None


def _match_examples(description: str, vectorizer, example_vectors, example_categories) -> dict:
    """Match description against few-shot examples."""
    try:
        query_vec = vectorizer.transform([description])
        similarities = cosine_similarity(query_vec, example_vectors).flatten()
        
        best_idx = np.argmax(similarities)
        best_score = similarities[best_idx]
        
        if best_score >= 0.5:
            return example_categories[best_idx]
    except Exception:
        pass
    
    return None


def _resolve_signals(signals: list, taxonomy: dict) -> tuple:
    """Resolve multiple signals into a single category with confidence."""
    if not signals:
        return {}, 0.30
    
    if len(signals) == 1:
        return signals[0][1], 0.60
    
    signal_types = [s[0] for s in signals]
    categories = [s[1] for s in signals]
    
    if len(set(tuple(c.items()) for c in categories)) == 1:
        if len(signals) >= 3:
            return categories[0], 0.95
        elif len(signals) == 2:
            return categories[0], 0.80
    
    category_counts = {}
    for cat in categories:
        key = (cat.get('l1'), cat.get('l2'), cat.get('l3'))
        category_counts[key] = category_counts.get(key, 0) + 1
    
    best_match = max(category_counts.items(), key=lambda x: x[1])
    agreement_count = best_match[1]
    
    if agreement_count >= 2:
        return {'l1': best_match[0][0], 'l2': best_match[0][1], 'l3': best_match[0][2]}, 0.80
    else:
        return {'l1': best_match[0][0], 'l2': best_match[0][1], 'l3': best_match[0][2]}, 0.60
