#!/usr/bin/env python3
"""Demo script showing common vendor dictionary functionality."""

import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from shared.vendor_matcher import VendorMatcher


def demo_basic_matching():
    """Demo 1: Basic matching with empty company vendor master."""
    print("=" * 70)
    print("DEMO 1: Basic Matching (Empty Company Vendor Master)")
    print("=" * 70)
    
    vendor_master = []
    matcher = VendorMatcher(vendor_master)
    
    test_cases = [
        'PwC',
        'KPMG',
        'Microsoft',
        'DHL',
        'Qantas',
        'NAB',
        'Deloite',  # typo
    ]
    
    for dirty_name in test_cases:
        matched, confidence, candidates = matcher.match(dirty_name)
        if matched:
            print(f"\n✓ '{dirty_name}' → {matched['supplier_name']}")
            print(f"  Confidence: {confidence:.2f}")
            print(f"  Supplier ID: {matched['supplier_id']}")
            print(f"  Category: {matched.get('category_l1', 'N/A')}")
        else:
            print(f"\n✗ '{dirty_name}' → No auto-match (confidence: {confidence:.2f})")
            if candidates:
                print(f"  Top candidate: {candidates[0]['vendor']['supplier_name']} ({candidates[0]['score']:.2f})")


def demo_abbreviations():
    """Demo 2: Abbreviation matching."""
    print("\n\n" + "=" * 70)
    print("DEMO 2: Abbreviation Matching")
    print("=" * 70)
    
    vendor_master = []
    matcher = VendorMatcher(vendor_master)
    
    abbreviation_tests = [
        ('PwC', 'PricewaterhouseCoopers'),
        ('EY', 'Ernst & Young'),
        ('AWS', 'Amazon Web Services'),
        ('BCG', 'Boston Consulting Group'),
        ('CBA', 'Commonwealth Bank'),
        ('HPE', 'Hewlett Packard Enterprise'),
    ]
    
    for abbr, expected in abbreviation_tests:
        matched, confidence, _ = matcher.match(abbr)
        if matched and matched['supplier_name'] == expected:
            print(f"✓ '{abbr}' → {expected}")
        else:
            print(f"✗ '{abbr}' → Failed (got: {matched['supplier_name'] if matched else 'None'})")


def demo_company_override():
    """Demo 3: Company-specific vendors override common vendors."""
    print("\n\n" + "=" * 70)
    print("DEMO 3: Company Vendor Override")
    print("=" * 70)
    
    vendor_master = [
        {'supplier_name': 'Microsoft Australia Pty Ltd', 'supplier_id': 'CUSTOM-MS-001'},
        {'supplier_name': 'KPMG Australia', 'supplier_id': 'CUSTOM-KPMG-001'},
    ]
    matcher = VendorMatcher(vendor_master)
    
    print("\nCompany has custom vendors for Microsoft and KPMG:")
    print("  - Microsoft Australia Pty Ltd (CUSTOM-MS-001)")
    print("  - KPMG Australia (CUSTOM-KPMG-001)")
    
    # Test exact match on company vendor
    matched, confidence, _ = matcher.match('Microsoft Australia Pty Ltd')
    print(f"\n✓ 'Microsoft Australia Pty Ltd' → {matched['supplier_name']}")
    print(f"  Supplier ID: {matched['supplier_id']} (company-specific)")
    
    # Test common vendor abbreviation (still uses common vendor)
    matched, confidence, _ = matcher.match('Microsoft')
    if matched:
        print(f"\n✓ 'Microsoft' → {matched['supplier_name']}")
        print(f"  Supplier ID: {matched['supplier_id']} ({'company-specific' if 'CUSTOM' in matched['supplier_id'] else 'common vendor'})")


def demo_cross_industry():
    """Demo 4: Cross-industry coverage."""
    print("\n\n" + "=" * 70)
    print("DEMO 4: Cross-Industry Coverage")
    print("=" * 70)
    
    vendor_master = []
    matcher = VendorMatcher(vendor_master)
    
    industry_examples = [
        ('Professional Services', 'Deloitte'),
        ('IT & Software', 'Salesforce'),
        ('Logistics & Freight', 'FedEx'),
        ('Banks & Finance', 'Westpac'),
        ('Travel', 'Virgin Australia'),
        ('Engineering', 'Jacobs'),
        ('Telecommunications', 'Telstra'),
        ('Safety & PPE', '3M'),
        ('Vehicles', 'Toyota'),
        ('Heavy Equipment', 'Caterpillar'),
    ]
    
    for category, vendor_name in industry_examples:
        matched, confidence, _ = matcher.match(vendor_name)
        if matched:
            print(f"✓ {category:25s} → {matched['supplier_name']:30s} ({matched.get('category_l1', 'N/A')})")
        else:
            print(f"✗ {category:25s} → {vendor_name} (not found)")


def demo_statistics():
    """Demo 5: Common vendor statistics."""
    print("\n\n" + "=" * 70)
    print("DEMO 5: Common Vendor Statistics")
    print("=" * 70)
    
    vendor_master = []
    matcher = VendorMatcher(vendor_master)
    
    print(f"\nTotal common vendors loaded: {len(matcher.common_vendors)}")
    print(f"Total vendors in combined master: {len(matcher.combined_vendor_master)}")
    
    # Count by category
    from collections import Counter
    categories = Counter(v.get('category_l1', 'Unknown') for v in matcher.combined_vendor_master)
    
    print("\nVendors by category:")
    for cat, count in sorted(categories.items(), key=lambda x: -x[1]):
        print(f"  {cat:30s}: {count:3d}")


if __name__ == '__main__':
    demo_basic_matching()
    demo_abbreviations()
    demo_company_override()
    demo_cross_industry()
    demo_statistics()
    
    print("\n\n" + "=" * 70)
    print("Demo complete! See COMMON_VENDORS_README.md for more details.")
    print("=" * 70)
