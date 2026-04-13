"""Fuzzy matches vendor names against a master list."""
from typing import Optional, Tuple, List
import pandas as pd
from rapidfuzz import fuzz
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np
import json
import os


class VendorMatcher:
    """Matches dirty vendor names to canonical vendor master list.
    
    Uses a three-stage approach:
    1. Check common_vendors.json (cross-industry baseline vendors)
    2. Check vendor_dictionary (learned mappings from previous runs)
    3. Fuzzy match against company-specific vendor_master using TF-IDF + rapidfuzz
    
    The common vendors dictionary provides out-of-the-box matching for ~200-300
    well-known companies across all industries (Big 4, IT, logistics, etc).
    """
    
    def __init__(self, vendor_master: List[dict], vendor_dictionary: Optional[dict] = None):
        """vendor_master: list of {supplier_name, supplier_id, ...} from config
        vendor_dictionary: dict of {dirty_name: canonical_name} from blob storage
        """
        self.vendor_master = vendor_master
        self.vendor_dictionary = vendor_dictionary or {}
        
        # Load common vendors dictionary
        self.common_vendors = self._load_common_vendors()
        
        # Build combined vendor master: common vendors + company-specific vendors
        self.combined_vendor_master = self._build_combined_master()
        
        self.vendor_names = [v.get("supplier_name", "") for v in self.combined_vendor_master]
        
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
    
    def _load_common_vendors(self) -> List[dict]:
        """Load common vendors from shared/common_vendors.json"""
        try:
            common_vendors_path = os.path.join(
                os.path.dirname(__file__),
                'common_vendors.json'
            )
            with open(common_vendors_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            return []
    
    def _build_combined_master(self) -> List[dict]:
        """Build combined vendor master with company-specific vendors first, then common.
        
        Company-specific vendors are prioritized in fuzzy matching by appearing first.
        Common vendors get synthetic supplier_ids like 'COMMON-001'.
        If a company-specific vendor has the same canonical name as a common vendor,
        the common vendor is skipped.
        """
        combined = []
        company_vendor_names = {v.get("supplier_name", "").lower() for v in self.vendor_master}
        
        # Add company-specific vendors FIRST (higher priority in fuzzy matching)
        for vendor in self.vendor_master:
            vendor_copy = vendor.copy()
            vendor_copy["source"] = "company_config"
            combined.append(vendor_copy)
        
        # Add common vendors (skip if already in company master)
        for idx, common_vendor in enumerate(self.common_vendors):
            canonical_name = common_vendor.get("canonical_name", "")
            if canonical_name.lower() not in company_vendor_names:
                combined.append({
                    "supplier_name": canonical_name,
                    "supplier_id": f"COMMON-{idx+1:03d}",
                    "category_l1": common_vendor.get("typical_category_l1", ""),
                    "source": "common_vendors"
                })
        
        return combined
    
    def _match_common_vendor_abbreviation(self, dirty_name: str) -> Optional[dict]:
        """Check if dirty_name matches any common vendor abbreviation.
        Returns vendor dict if found, None otherwise.
        
        Only matches if there's no exact match in company-specific vendors first.
        """
        dirty_lower = dirty_name.lower().strip()
        
        # First check if there's an exact match in company-specific vendors
        for vendor in self.vendor_master:
            if vendor.get("supplier_name", "").lower().strip() == dirty_lower:
                # Found exact match in company vendors, don't use common vendor
                return None
        
        # Now check common vendor abbreviations
        for common_vendor in self.common_vendors:
            abbreviations = common_vendor.get("common_abbreviations", [])
            for abbr in abbreviations:
                if dirty_lower == abbr.lower().strip():
                    # Find this vendor in combined_vendor_master
                    canonical_name = common_vendor.get("canonical_name", "")
                    for vendor in self.combined_vendor_master:
                        if vendor.get("supplier_name") == canonical_name and vendor.get("source") == "common_vendors":
                            return vendor
        return None
    
    def match(self, dirty_name: str) -> Tuple[Optional[dict], float, List[dict]]:
        """Match a single vendor name.
        Returns: (best_match_vendor_dict_or_None, confidence, top_3_candidates)
        
        confidence >= 0.90: auto-resolve (return matched vendor)
        0.70 <= confidence < 0.90: flag for LLM review (return candidates)
        confidence < 0.70: flag as unknown (return candidates anyway)
        
        Matching priority:
        1. Check common_vendors abbreviations for instant resolution
        2. Check vendor_dictionary (learned mappings) for instant resolution
        3. Fuzzy match against combined vendor master (common + company-specific)
        """
        if not dirty_name or not isinstance(dirty_name, str):
            return None, 0.0, []
        
        dirty_name = dirty_name.strip()
        
        # Check common vendors abbreviations first
        matched_common = self._match_common_vendor_abbreviation(dirty_name)
        if matched_common:
            return matched_common, 1.0, [{"vendor": matched_common, "score": 1.0, "method": "common_vendor_abbreviation"}]
        
        # Check vendor_dictionary (learned mappings)
        if dirty_name in self.vendor_dictionary:
            canonical_name = self.vendor_dictionary[dirty_name]
            for vendor in self.combined_vendor_master:
                if vendor.get("supplier_name") == canonical_name:
                    return vendor, 1.0, [{"vendor": vendor, "score": 1.0, "method": "dictionary"}]
        
        if not self.combined_vendor_master:
            return None, 0.0, []
        
        candidates = []
        
        if self.vectorizer and self.tfidf_matrix is not None:
            try:
                query_vec = self.vectorizer.transform([dirty_name])
                similarities = cosine_similarity(query_vec, self.tfidf_matrix).flatten()
                top_indices = np.argsort(similarities)[-20:][::-1]
                
                for idx in top_indices:
                    vendor = self.combined_vendor_master[idx]
                    vendor_name = vendor.get("supplier_name", "")
                    
                    score = fuzz.token_sort_ratio(dirty_name.lower(), vendor_name.lower()) / 100.0
                    
                    candidates.append({
                        "vendor": vendor,
                        "score": score,
                        "method": "fuzzy"
                    })
            except Exception:
                for vendor in self.combined_vendor_master:
                    vendor_name = vendor.get("supplier_name", "")
                    score = fuzz.token_sort_ratio(dirty_name.lower(), vendor_name.lower()) / 100.0
                    candidates.append({
                        "vendor": vendor,
                        "score": score,
                        "method": "fuzzy"
                    })
        else:
            for vendor in self.combined_vendor_master:
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
