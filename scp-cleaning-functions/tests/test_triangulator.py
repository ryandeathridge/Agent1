"""Tests for triangulator module."""
import pytest
import pandas as pd
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from shared.triangulator import triangulate_amount_qty_price, triangulate_dataframe


def test_derive_unit_price():
    """Test deriving unit_price from amount and quantity."""
    amount = 1500.0
    quantity = 10.0
    unit_price = None
    
    new_amount, new_quantity, new_unit_price, was_derived, method = triangulate_amount_qty_price(
        amount, quantity, unit_price
    )
    
    assert was_derived is True
    assert method == "derived_unit_price"
    assert new_amount == 1500.0
    assert new_quantity == 10.0
    assert new_unit_price == 150.00


def test_derive_quantity():
    """Test deriving quantity from amount and unit_price."""
    amount = 1500.0
    quantity = None
    unit_price = 150.0
    
    new_amount, new_quantity, new_unit_price, was_derived, method = triangulate_amount_qty_price(
        amount, quantity, unit_price
    )
    
    assert was_derived is True
    assert method == "derived_quantity"
    assert new_amount == 1500.0
    assert new_quantity == 10.0
    assert new_unit_price == 150.0


def test_derive_amount():
    """Test deriving amount from quantity and unit_price."""
    amount = None
    quantity = 10.0
    unit_price = 150.0
    
    new_amount, new_quantity, new_unit_price, was_derived, method = triangulate_amount_qty_price(
        amount, quantity, unit_price
    )
    
    assert was_derived is True
    assert method == "derived_amount"
    assert new_amount == 1500.00
    assert new_quantity == 10.0
    assert new_unit_price == 150.0


def test_two_nulls_no_derivation():
    """Test that 2 nulls does NOT derive."""
    amount = None
    quantity = None
    unit_price = 50.0
    
    new_amount, new_quantity, new_unit_price, was_derived, method = triangulate_amount_qty_price(
        amount, quantity, unit_price
    )
    
    assert was_derived is False
    assert method == ""
    assert new_amount is None
    assert new_quantity is None
    assert new_unit_price == 50.0


def test_three_nulls_no_derivation():
    """Test that 3 nulls does NOT derive."""
    amount = None
    quantity = None
    unit_price = None
    
    new_amount, new_quantity, new_unit_price, was_derived, method = triangulate_amount_qty_price(
        amount, quantity, unit_price
    )
    
    assert was_derived is False
    assert method == ""
    assert new_amount is None
    assert new_quantity is None
    assert new_unit_price is None


def test_zero_nulls_no_derivation():
    """Test that 0 nulls does NOT derive (all fields present)."""
    amount = 1500.0
    quantity = 10.0
    unit_price = 150.0
    
    new_amount, new_quantity, new_unit_price, was_derived, method = triangulate_amount_qty_price(
        amount, quantity, unit_price
    )
    
    assert was_derived is False
    assert method == ""
    assert new_amount == 1500.0
    assert new_quantity == 10.0
    assert new_unit_price == 150.0


def test_division_by_zero_quantity():
    """Test that division by zero in quantity doesn't crash."""
    amount = 1500.0
    quantity = 0.0
    unit_price = None
    
    new_amount, new_quantity, new_unit_price, was_derived, method = triangulate_amount_qty_price(
        amount, quantity, unit_price
    )
    
    assert was_derived is False
    assert new_unit_price is None


def test_division_by_zero_unit_price():
    """Test that division by zero in unit_price doesn't crash."""
    amount = 1500.0
    quantity = None
    unit_price = 0.0
    
    new_amount, new_quantity, new_unit_price, was_derived, method = triangulate_amount_qty_price(
        amount, quantity, unit_price
    )
    
    assert was_derived is False
    assert new_quantity is None


def test_rounding_unit_price():
    """Test that unit_price is rounded to 2 decimal places."""
    amount = 100.0
    quantity = 3.0
    unit_price = None
    
    new_amount, new_quantity, new_unit_price, was_derived, method = triangulate_amount_qty_price(
        amount, quantity, unit_price
    )
    
    assert was_derived is True
    assert new_unit_price == 33.33  # 100/3 = 33.333... rounded to 2 decimals


def test_rounding_quantity():
    """Test that quantity is rounded to 4 decimal places."""
    amount = 100.0
    quantity = None
    unit_price = 3.0
    
    new_amount, new_quantity, new_unit_price, was_derived, method = triangulate_amount_qty_price(
        amount, quantity, unit_price
    )
    
    assert was_derived is True
    assert new_quantity == 33.3333  # 100/3 = 33.333... rounded to 4 decimals


def test_dataframe_triangulation():
    """Test triangulation on a dataframe."""
    df = pd.DataFrame({
        'record_id': ['REC-001', 'REC-002', 'REC-003', 'REC-004'],
        'amount': [1500.0, None, 2000.0, None],
        'quantity': [10.0, 20.0, None, None],
        'unit_price': [None, 50.0, 100.0, None]
    })
    
    processed_df, human_review_df, changes_log = triangulate_dataframe(df)
    
    # REC-001: should derive unit_price = 150.00
    # REC-002: should derive amount = 1000.00
    # REC-003: should derive quantity = 20.0
    # REC-004: should go to human_review (2 nulls)
    
    assert len(processed_df) == 3
    assert len(human_review_df) == 1
    assert len(changes_log) == 3
    
    # Check that REC-004 is in human_review
    assert 'REC-004' in human_review_df['record_id'].values
    
    # Check that derivations were logged
    assert all(change['method'] == 'derived' for change in changes_log)
    assert all(change['confidence'] == 0.95 for change in changes_log)
    assert all(change['agent'] == 'triangulator' for change in changes_log)


def test_dataframe_with_missing_columns():
    """Test triangulation when columns don't exist in dataframe."""
    df = pd.DataFrame({
        'record_id': ['REC-001'],
        'other_field': ['value']
    })
    
    processed_df, human_review_df, changes_log = triangulate_dataframe(df)
    
    # Should create the columns and flag for human review
    assert 'amount' in processed_df.columns
    assert 'quantity' in processed_df.columns
    assert 'unit_price' in processed_df.columns
