"""Tests for validate_output function."""
import pytest
import pandas as pd
import json
from unittest.mock import Mock, patch
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from validate_output import main
import azure.functions as func


def test_validate_output_clean_data():
    """Test validation of clean data."""
    test_data = pd.DataFrame({
        'record_id': ['BHP-PO-0000001', 'BHP-PO-0000002'],
        'date': ['2025-01-15', '2025-01-16'],
        'amount': [1500.00, 2500.50],
        'supplier_name': ['Caterpillar Inc', 'Komatsu Australia'],
        'supplier_id': ['SUP-00001', 'SUP-00002'],
        'currency': ['AUD', 'AUD']
    })
    
    config = {
        'category_taxonomy': {
            'Equipment & Parts': {
                'Mobile Equipment': ['Haul truck parts', 'Excavator parts']
            }
        },
        'valid_currencies': ['AUD', 'USD', 'CLP']
    }
    
    with patch('validate_output.download_dataframe', return_value=test_data):
        req_body = {
            'blob_url': 'https://test.blob.core.windows.net/test.parquet',
            'config': config
        }
        
        req = Mock(spec=func.HttpRequest)
        req.get_json.return_value = req_body
        
        response = main(req)
        
        assert response.status_code == 200
        
        result = json.loads(response.get_body().decode())
        assert 'is_valid' in result
        assert 'total_records' in result
        assert result['total_records'] == 2


def test_validate_output_with_errors():
    """Test validation detects errors."""
    test_data = pd.DataFrame({
        'record_id': ['INVALID-ID', 'BHP-PO-0000002'],
        'date': ['2025-01-15', 'invalid-date'],
        'amount': [1500.00, -2500.50],
        'supplier_name': ['Caterpillar Inc', 'Komatsu Australia'],
        'currency': ['AUD', 'INVALID']
    })
    
    config = {
        'category_taxonomy': {},
        'valid_currencies': ['AUD', 'USD', 'CLP']
    }
    
    with patch('validate_output.download_dataframe', return_value=test_data):
        req_body = {
            'blob_url': 'https://test.blob.core.windows.net/test.parquet',
            'config': config
        }
        
        req = Mock(spec=func.HttpRequest)
        req.get_json.return_value = req_body
        
        response = main(req)
        
        result = json.loads(response.get_body().decode())
        assert result['error_count'] > 0


def test_validate_output_outliers():
    """Test outlier detection."""
    amounts = [1000] * 50 + [100000]
    
    test_data = pd.DataFrame({
        'record_id': [f'BHP-PO-{i:07d}' for i in range(51)],
        'date': ['2025-01-15'] * 51,
        'amount': amounts,
        'supplier_name': ['Caterpillar Inc'] * 51,
        'category_l1': ['Equipment & Parts'] * 51
    })
    
    config = {
        'category_taxonomy': {
            'Equipment & Parts': {
                'Mobile Equipment': ['Haul truck parts']
            }
        }
    }
    
    with patch('validate_output.download_dataframe', return_value=test_data):
        req_body = {
            'blob_url': 'https://test.blob.core.windows.net/test.parquet',
            'config': config
        }
        
        req = Mock(spec=func.HttpRequest)
        req.get_json.return_value = req_body
        
        response = main(req)
        
        result = json.loads(response.get_body().decode())
        assert len(result['outliers']) > 0
