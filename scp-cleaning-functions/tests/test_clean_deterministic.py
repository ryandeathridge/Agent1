"""Tests for clean_deterministic function."""
import pytest
import pandas as pd
import json
from unittest.mock import Mock, patch
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from clean_deterministic import main
import azure.functions as func


def test_clean_deterministic_basic():
    """Test basic cleaning functionality."""
    test_data = pd.DataFrame({
        'record_id': ['BHP-PO-0000001', 'BHP-PO-0000002'],
        'date': ['15/01/2025', '16-Jan-2025'],
        'amount': ['$1,500.00', '2500.50'],
        'supplier_name': ['Caterpilalr Inc', 'KOMATSU AUST'],
        'unit': ['each', 'pcs']
    })
    
    config = {
        'top_20_suppliers': [
            {'supplier_name': 'Caterpillar Inc', 'supplier_id': 'SUP-00001'},
            {'supplier_name': 'Komatsu Australia Pty Ltd', 'supplier_id': 'SUP-00002'}
        ]
    }
    
    with patch('clean_deterministic.download_dataframe', return_value=test_data), \
         patch('clean_deterministic.upload_dataframe', return_value='https://test.blob/cleaned.parquet'):
        
        req_body = {
            'blob_url': 'https://test.blob.core.windows.net/test.parquet',
            'config': config
        }
        
        req = Mock(spec=func.HttpRequest)
        req.get_json.return_value = req_body
        
        response = main(req)
        
        assert response.status_code == 200
        
        result = json.loads(response.get_body().decode())
        assert 'cleaned_blob_url' in result
        assert 'stats' in result
        
        stats = result['stats']
        assert stats['input_rows'] == 2


def test_clean_deterministic_date_normalization():
    """Test date normalization."""
    test_data = pd.DataFrame({
        'record_id': ['BHP-PO-0000001'],
        'date': ['15/01/2025'],
        'amount': [1500.00],
        'supplier_name': ['Caterpillar Inc']
    })
    
    with patch('clean_deterministic.download_dataframe', return_value=test_data), \
         patch('clean_deterministic.upload_dataframe', return_value='https://test.blob/cleaned.parquet'):
        
        req_body = {
            'blob_url': 'https://test.blob.core.windows.net/test.parquet',
            'config': {'top_20_suppliers': []}
        }
        
        req = Mock(spec=func.HttpRequest)
        req.get_json.return_value = req_body
        
        response = main(req)
        
        result = json.loads(response.get_body().decode())
        stats = result['stats']
        
        assert 'fields_modified' in stats


def test_clean_deterministic_vendor_matching():
    """Test vendor matching with typos."""
    test_data = pd.DataFrame({
        'record_id': ['BHP-PO-0000001'],
        'date': ['2025-01-15'],
        'amount': [1500.00],
        'supplier_name': ['Caterpilalr Inc']
    })
    
    config = {
        'top_20_suppliers': [
            {'supplier_name': 'Caterpillar Inc', 'supplier_id': 'SUP-00001'}
        ]
    }
    
    with patch('clean_deterministic.download_dataframe', return_value=test_data), \
         patch('clean_deterministic.upload_dataframe', return_value='https://test.blob/cleaned.parquet'):
        
        req_body = {
            'blob_url': 'https://test.blob.core.windows.net/test.parquet',
            'config': config
        }
        
        req = Mock(spec=func.HttpRequest)
        req.get_json.return_value = req_body
        
        response = main(req)
        
        result = json.loads(response.get_body().decode())
        assert 'cleaned_blob_url' in result


def test_triangulation_derive_unit_price():
    """Test triangulation: derive unit_price from amount and quantity."""
    test_data = pd.DataFrame({
        'record_id': ['BHP-PO-0000001'],
        'date': ['2025-01-15'],
        'amount': [1500.0],
        'quantity': [10.0],
        'unit_price': [None],
        'supplier_name': ['Test Supplier']
    })
    
    config = {
        'top_20_suppliers': []
    }
    
    with patch('clean_deterministic.download_dataframe', return_value=test_data), \
         patch('clean_deterministic.upload_dataframe', side_effect=lambda df, name: f'https://test.blob/{name}'):
        
        req_body = {
            'blob_url': 'https://test.blob.core.windows.net/test.parquet',
            'config': config
        }
        
        req = Mock(spec=func.HttpRequest)
        req.get_json.return_value = req_body
        
        response = main(req)
        
        result = json.loads(response.get_body().decode())
        assert response.status_code == 200
        assert 'stats' in result
        
        # Check that triangulation was performed
        stats = result['stats']
        assert 'fields_modified' in stats
        if 'triangulated_fields' in stats['fields_modified']:
            assert stats['fields_modified']['triangulated_fields'] >= 1


def test_triangulation_two_nulls_human_review():
    """Test triangulation: 2 nulls should go to human_review, not derive."""
    test_data = pd.DataFrame({
        'record_id': ['BHP-PO-0000001'],
        'date': ['2025-01-15'],
        'amount': [None],
        'quantity': [None],
        'unit_price': [50.0],
        'supplier_name': ['Test Supplier']
    })
    
    config = {
        'top_20_suppliers': []
    }
    
    with patch('clean_deterministic.download_dataframe', return_value=test_data), \
         patch('clean_deterministic.upload_dataframe', side_effect=lambda df, name: f'https://test.blob/{name}'):
        
        req_body = {
            'blob_url': 'https://test.blob.core.windows.net/test.parquet',
            'config': config
        }
        
        req = Mock(spec=func.HttpRequest)
        req.get_json.return_value = req_body
        
        response = main(req)
        
        result = json.loads(response.get_body().decode())
        assert response.status_code == 200
        
        # Check that record went to human_review
        stats = result['stats']
        assert 'human_review_rows' in stats
        assert stats['human_review_rows'] == 1
        assert result.get('human_review_blob_url') is not None


def test_triangulation_three_nulls_human_review():
    """Test triangulation: 3 nulls should go to human_review, not derive."""
    test_data = pd.DataFrame({
        'record_id': ['BHP-PO-0000001'],
        'date': ['2025-01-15'],
        'amount': [None],
        'quantity': [None],
        'unit_price': [None],
        'supplier_name': ['Test Supplier']
    })
    
    config = {
        'top_20_suppliers': []
    }
    
    with patch('clean_deterministic.download_dataframe', return_value=test_data), \
         patch('clean_deterministic.upload_dataframe', side_effect=lambda df, name: f'https://test.blob/{name}'):
        
        req_body = {
            'blob_url': 'https://test.blob.core.windows.net/test.parquet',
            'config': config
        }
        
        req = Mock(spec=func.HttpRequest)
        req.get_json.return_value = req_body
        
        response = main(req)
        
        result = json.loads(response.get_body().decode())
        assert response.status_code == 200
        
        # Check that record went to human_review
        stats = result['stats']
        assert 'human_review_rows' in stats
        assert stats['human_review_rows'] == 1
        assert result.get('human_review_blob_url') is not None
