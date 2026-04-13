"""Fuzzy matches vendor names against a master list."""
from typing import Optional, Tuple, List
import pandas as pd
from rapidfuzz import fuzz
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np


class VendorMatcher:
    """Matches dirty vendor names to canonical vendor master list.
    
    Uses a two-stage approach:
    1. TF-IDF pre-filtering to find candidate matches (fast, narrows search space)
    2. Token-sort ratio from rapidfuzz for final scoring
    
    Also checks the vendor_dictionary (learned mappings from previous runs) first
    for instant resolution of known variants.
    """
    
    def __init__(self, vendor_master: List[dict], vendor_dictionary: Optional[dict] = None):
        """vendor_master: list of {supplier_name, supplier_id, ...} from config
        vendor_dictionary: dict of {dirty_name: canonical_name} from blob storage
        """
        self.vendor_master = vendor_master
        self.vendor_dictionary = vendor_dictionary or {}
        
        self.vendor_names = [v.get("supplier_name", "") for v in vendor_master]
        
        if self.vendor_names:
            self.vectorizer = TfidfVectorizer(
                analyzer='char_wb',
                ngram_range=(2, 3),
                lowercase=True
            )
            self.tfidf_matrix = self.vectorizer.fit_transform(self.vendor_names)
        else:
            self.vectorizer = None
            self.tfidf_matrix = None
    
    def match(self, dirty_name: str) -> Tuple[Optional[dict], float, List[dict]]:
        """Match a single vendor name.
        Returns: (best_match_vendor_dict_or_None, confidence, top_3_candidates)
        
        confidence >= 0.90: auto-resolve (return matched vendor)
        0.70 <= confidence < 0.90: flag for LLM review (return candidates)
        confidence < 0.70: flag as unknown (return candidates anyway)
        
        Check vendor_dictionary first for instant resolution.
        Then TF-IDF pre-filter to top 20 candidates.
        Then rapidfuzz token_sort_ratio for final scoring.
        """
        if not dirty_name or not isinstance(dirty_name, str):
            return None, 0.0, []
        
        dirty_name = dirty_name.strip()
        
        if dirty_name in self.vendor_dictionary:
            canonical_name = self.vendor_dictionary[dirty_name]
            for vendor in self.vendor_master:
                if vendor.get("supplier_name") == canonical_name:
                    return vendor, 1.0, [{"vendor": vendor, "score": 1.0, "method": "dictionary"}]
        
        if not self.vendor_master:
            return None, 0.0, []
        
        candidates = []
        
        if self.vectorizer and self.tfidf_matrix is not None:
            try:
                query_vec = self.vectorizer.transform([dirty_name])
                similarities = cosine_similarity(query_vec, self.tfidf_matrix).flatten()
                top_indices = np.argsort(similarities)[-20:][::-1]
                
                for idx in top_indices:
                    vendor = self.vendor_master[idx]
                    vendor_name = vendor.get("supplier_name", "")
                    
                    score = fuzz.token_sort_ratio(dirty_name.lower(), vendor_name.lower()) / 100.0
                    
                    candidates.append({
                        "vendor": vendor,
                        "score": score,
                        "method": "fuzzy"
                    })
            except Exception:
                for vendor in self.vendor_master:
                    vendor_name = vendor.get("supplier_name", "")
                    score = fuzz.token_sort_ratio(dirty_name.lower(), vendor_name.lower()) / 100.0
                    candidates.append({
                        "vendor": vendor,
                        "score": score,
                        "method": "fuzzy"
                    })
        else:
            for vendor in self.vendor_master:
                vendor_name = vendor.get("supplier_name", "")
                score = fuzz.token_sort_ratio(dirty_name.lower(), vendor_name.lower()) / 100.0
                candidates.append({
                    "vendor": vendor,
                    "score": score,
                    "method": "fuzzy"
                })
        
        candidates.sort(key=lambda x: x["score"], reverse=True)
        top_3 = candidates[:3]
        
        if not top_3:
            return None, 0.0, []
        
        best = top_3[0]
        confidence = best["score"]
        
        if confidence >= 0.90:
            return best["vendor"], confidence, top_3
        else:
            return None, confidence, top_3
    
    def match_column(self, series: pd.Series) -> pd.DataFrame:
        """Batch match entire column. Returns DataFrame with:
        'matched_name', 'matched_id', 'confidence', 'candidates_json', 'method'
        method is one of: 'dictionary', 'fuzzy_auto', 'fuzzy_flagged', 'unknown'
        """
        results = []
        
        for value in series:
            matched_vendor, confidence, candidates = self.match(value)
            
            if matched_vendor:
                method = 'dictionary' if confidence == 1.0 else 'fuzzy_auto'
                results.append({
                    'matched_name': matched_vendor.get('supplier_name'),
                    'matched_id': matched_vendor.get('supplier_id'),
                    'confidence': confidence,
                    'candidates_json': candidates,
                    'method': method
                })
            elif candidates and confidence >= 0.70:
                results.append({
                    'matched_name': None,
                    'matched_id': None,
                    'confidence': confidence,
                    'candidates_json': candidates,
                    'method': 'fuzzy_flagged'
                })
            else:
                results.append({
                    'matched_name': None,
                    'matched_id': None,
                    'confidence': confidence,
                    'candidates_json': candidates,
                    'method': 'unknown'
                })
        
        return pd.DataFrame(results)
