"""Tests for common vendors dictionary functionality."""
import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from shared.vendor_matcher import VendorMatcher


def test_common_vendors_loaded():
    """Test that common vendors are loaded successfully."""
    vendor_master = []
    matcher = VendorMatcher(vendor_master)
    
    assert len(matcher.common_vendors) > 0
    assert len(matcher.common_vendors) >= 200


def test_common_vendor_abbreviation_match():
    """Test exact match on common vendor abbreviations."""
    vendor_master = []
    matcher = VendorMatcher(vendor_master)
    
    # Test PwC abbreviation
    matched, confidence, candidates = matcher.match('PwC')
    assert matched is not None
    assert matched['supplier_name'] == 'PricewaterhouseCoopers'
    assert confidence == 1.0
    
    # Test KPMG
    matched, confidence, candidates = matcher.match('KPMG')
    assert matched is not None
    assert matched['supplier_name'] == 'KPMG'
    assert confidence == 1.0
    
    # Test Microsoft
    matched, confidence, candidates = matcher.match('Microsoft')
    assert matched is not None
    assert matched['supplier_name'] == 'Microsoft Corporation'
    assert confidence == 1.0


def test_common_vendor_case_insensitive():
    """Test that abbreviation matching is case-insensitive."""
    vendor_master = []
    matcher = VendorMatcher(vendor_master)
    
    # Test lowercase
    matched, confidence, candidates = matcher.match('pwc')
    assert matched is not None
    assert matched['supplier_name'] == 'PricewaterhouseCoopers'
    
    # Test uppercase
    matched, confidence, candidates = matcher.match('PWC')
    assert matched is not None
    assert matched['supplier_name'] == 'PricewaterhouseCoopers'
    
    # Test mixed case
    matched, confidence, candidates = matcher.match('PwC')
    assert matched is not None
    assert matched['supplier_name'] == 'PricewaterhouseCoopers'


def test_common_vendor_multiple_abbreviations():
    """Test vendors with multiple abbreviations."""
    vendor_master = []
    matcher = VendorMatcher(vendor_master)
    
    # Test different abbreviations for Ernst & Young
    matched1, conf1, _ = matcher.match('EY')
    matched2, conf2, _ = matcher.match('Ernst & Young')
    matched3, conf3, _ = matcher.match('E&Y')
    
    assert matched1 is not None
    assert matched2 is not None
    assert matched3 is not None
    
    # All should resolve to same canonical name
    assert matched1['supplier_name'] == 'Ernst & Young'
    assert matched2['supplier_name'] == 'Ernst & Young'
    assert matched3['supplier_name'] == 'Ernst & Young'


def test_common_vendors_have_category():
    """Test that common vendors include category information."""
    vendor_master = []
    matcher = VendorMatcher(vendor_master)
    
    matched, confidence, candidates = matcher.match('KPMG')
    assert matched is not None
    assert 'category_l1' in matched
    assert matched['category_l1'] == 'Professional Services'


def test_common_vendors_have_supplier_id():
    """Test that common vendors get synthetic supplier IDs."""
    vendor_master = []
    matcher = VendorMatcher(vendor_master)
    
    matched, confidence, candidates = matcher.match('KPMG')
    assert matched is not None
    assert 'supplier_id' in matched
    assert matched['supplier_id'].startswith('COMMON-')


def test_company_vendor_overrides_common():
    """Test that company-specific vendors override common vendors."""
    vendor_master = [
        {'supplier_name': 'Microsoft Corporation', 'supplier_id': 'CUSTOM-MS-001'}
    ]
    matcher = VendorMatcher(vendor_master)
    
    # Exact match on company vendor name should use company vendor
    matched, confidence, candidates = matcher.match('Microsoft Corporation')
    assert matched is not None
    assert matched['supplier_name'] == 'Microsoft Corporation'
    # Should use company-specific ID, not COMMON-xxx
    assert matched['supplier_id'] == 'CUSTOM-MS-001'
    assert matched.get('source') == 'company_config'
    
    # Fuzzy match should prefer company vendor as top candidate (it's listed first)
    matched2, confidence2, candidates2 = matcher.match('Microsoft Corp')
    # Might not auto-match if confidence < 0.90, but should be top candidate
    assert len(candidates2) > 0
    assert candidates2[0]['vendor']['supplier_id'] == 'CUSTOM-MS-001'
    assert candidates2[0]['vendor']['supplier_name'] == 'Microsoft Corporation'


def test_common_and_company_vendors_combined():
    """Test that common vendors and company vendors work together."""
    vendor_master = [
        {'supplier_name': 'Mining Company ABC', 'supplier_id': 'SUP-001'}
    ]
    matcher = VendorMatcher(vendor_master)
    
    # Should match common vendor
    matched1, conf1, _ = matcher.match('KPMG')
    assert matched1 is not None
    assert matched1['supplier_name'] == 'KPMG'
    
    # Should match company-specific vendor
    matched2, conf2, _ = matcher.match('Mining Company ABC')
    assert matched2 is not None
    assert matched2['supplier_id'] == 'SUP-001'


def test_fuzzy_match_with_common_vendors():
    """Test fuzzy matching works with common vendors."""
    vendor_master = []
    matcher = VendorMatcher(vendor_master)
    
    # Slight typo in common vendor name
    matched, confidence, candidates = matcher.match('Deloite')
    
    # Should fuzzy match to Deloitte
    assert len(candidates) > 0
    assert candidates[0]['vendor']['supplier_name'] == 'Deloitte'
    assert confidence > 0.80


def test_vendor_dictionary_still_works():
    """Test that vendor_dictionary still works with common vendors."""
    vendor_master = []
    vendor_dictionary = {
        'CAT INC': 'Caterpillar'
    }
    matcher = VendorMatcher(vendor_master, vendor_dictionary)
    
    matched, confidence, candidates = matcher.match('CAT INC')
    assert matched is not None
    assert matched['supplier_name'] == 'Caterpillar'
    assert confidence == 1.0


def test_common_vendors_cross_industry():
    """Test that common vendors cover multiple industries."""
    vendor_master = []
    matcher = VendorMatcher(vendor_master)
    
    # Professional services
    matched1, _, _ = matcher.match('KPMG')
    assert matched1 is not None
    assert matched1['category_l1'] == 'Professional Services'
    
    # IT & Software
    matched2, _, _ = matcher.match('SAP')
    assert matched2 is not None
    assert matched2['category_l1'] == 'IT & Software'
    
    # Logistics
    matched3, _, _ = matcher.match('DHL')
    assert matched3 is not None
    assert matched3['category_l1'] == 'Logistics & Freight'
    
    # Banks
    matched4, _, _ = matcher.match('NAB')
    assert matched4 is not None
    assert matched4['category_l1'] == 'Banks & Finance'
    
    # Travel
    matched5, _, _ = matcher.match('Qantas')
    assert matched5 is not None
    assert matched5['category_l1'] == 'Travel'


def test_hilux_as_vendor():
    """Test that Hilux (vehicle model) is recognized as a vendor."""
    vendor_master = []
    matcher = VendorMatcher(vendor_master)
    
    matched, confidence, candidates = matcher.match('Hilux')
    assert matched is not None
    assert matched['supplier_name'] == 'Hilux'
    assert matched['category_l1'] == 'Vehicles'


def test_common_vendors_australian_focus():
    """Test Australian-specific vendors are included."""
    vendor_master = []
    matcher = VendorMatcher(vendor_master)
    
    # Australian banks
    matched1, _, _ = matcher.match('NAB')
    assert matched1 is not None
    
    matched2, _, _ = matcher.match('CBA')
    assert matched2 is not None
    
    # Australian logistics
    matched3, _, _ = matcher.match('Toll')
    assert matched3 is not None
    
    matched4, _, _ = matcher.match('Linfox')
    assert matched4 is not None
    
    # Australian utilities
    matched5, _, _ = matcher.match('AGL')
    assert matched5 is not None


def test_empty_vendor_master_with_common_vendors():
    """Test that matcher works with empty company vendor master."""
    vendor_master = []
    matcher = VendorMatcher(vendor_master)
    
    # Should still match common vendors
    matched, confidence, candidates = matcher.match('Microsoft')
    assert matched is not None
    assert confidence == 1.0
