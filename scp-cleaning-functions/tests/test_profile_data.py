"""Tests for profile_data function."""
import pytest
import pandas as pd
import json
from unittest.mock import Mock, patch
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from profile_data import main
import azure.functions as func


def test_profile_data_basic():
    """Test basic profiling functionality."""
    test_data = pd.DataFrame({
        'record_id': ['BHP-PO-0000001', 'BHP-PO-0000002', 'BHP-PO-0000003'],
        'date': ['2025-01-15', '2025-01-16', '2025-01-17'],
        'amount': [1500.00, 2500.50, 3000.00],
        'supplier_name': ['Caterpillar Inc', 'Komatsu Australia', 'Caterpillar Inc'],
        'description': ['Hydraulic hose', 'Excavator parts', 'Hydraulic hose']
    })
    
    with patch('profile_data.download_dataframe', return_value=test_data):
        req_body = {
            'blob_url': 'https://test.blob.core.windows.net/test.parquet',
            'config': {}
        }
        
        req = Mock(spec=func.HttpRequest)
        req.get_json.return_value = req_body
        
        response = main(req)
        
        assert response.status_code == 200
        
        result = json.loads(response.get_body().decode())
        assert 'profile' in result
        
        profile = result['profile']
        assert profile['total_rows'] == 3
        assert profile['total_columns'] == 5
        assert len(profile['columns']) == 5


def test_profile_data_with_duplicates():
    """Test profiling detects duplicates."""
    test_data = pd.DataFrame({
        'record_id': ['BHP-PO-0000001', 'BHP-PO-0000002', 'BHP-PO-0000001'],
        'date': ['2025-01-15', '2025-01-16', '2025-01-15'],
        'amount': [1500.00, 2500.50, 1500.00],
        'supplier_name': ['Caterpillar Inc', 'Komatsu Australia', 'Caterpillar Inc']
    })
    
    with patch('profile_data.download_dataframe', return_value=test_data):
        req_body = {
            'blob_url': 'https://test.blob.core.windows.net/test.parquet',
            'config': {}
        }
        
        req = Mock(spec=func.HttpRequest)
        req.get_json.return_value = req_body
        
        response = main(req)
        
        result = json.loads(response.get_body().decode())
        profile = result['profile']
        
        assert profile['duplicate_count'] > 0


def test_profile_data_missing_blob_url():
    """Test error handling for missing blob_url."""
    req = Mock(spec=func.HttpRequest)
    req.get_json.return_value = {}
    
    response = main(req)
    
    assert response.status_code == 400
    result = json.loads(response.get_body().decode())
    assert 'error' in result
