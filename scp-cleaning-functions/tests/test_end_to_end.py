"""End-to-end integration tests."""
import pytest
import pandas as pd
import json
from unittest.mock import Mock, patch
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))


def create_test_data():
    """Create synthetic test data."""
    return pd.DataFrame({
        'record_id': [f'BHP-PO-{i:07d}' for i in range(1, 101)],
        'date': ['15/01/2025'] * 50 + ['2025-01-16'] * 50,
        'amount': ['$1,500.00'] * 30 + [2500.50] * 70,
        'supplier_name': ['Caterpilalr Inc'] * 40 + ['KOMATSU AUST'] * 30 + ['Orica Ltd'] * 30,
        'description': ['Hydraulic hose'] * 40 + ['Excavator parts'] * 30 + ['Explosives'] * 30,
        'unit': ['each'] * 50 + ['pcs'] * 50,
        'currency': ['AUD'] * 100
    })


def create_test_config():
    """Create test configuration."""
    return {
        'top_20_suppliers': [
            {'supplier_name': 'Caterpillar Inc', 'supplier_id': 'SUP-00001'},
            {'supplier_name': 'Komatsu Australia Pty Ltd', 'supplier_id': 'SUP-00002'},
            {'supplier_name': 'Orica Ltd', 'supplier_id': 'SUP-00003'}
        ],
        'category_taxonomy': {
            'Equipment & Parts': {
                'Mobile Equipment': ['Haul truck parts', 'Excavator parts']
            },
            'Raw Materials & Consumables': {
                'Chemical Reagents': ['Explosives', 'Sulphuric acid']
            }
        },
        'valid_currencies': ['AUD', 'USD', 'CLP'],
        'units': ['EA', 'L', 'KG', 'T', 'M', 'HR']
    }


@pytest.mark.integration
def test_full_pipeline():
    """Test complete pipeline: profile -> clean -> validate -> format."""
    test_data = create_test_data()
    config = create_test_config()
    
    from profile_data import main as profile_main
    from clean_deterministic import main as clean_main
    from validate_output import main as validate_main
    from format_output import main as format_main
    
    with patch('profile_data.download_dataframe', return_value=test_data):
        req = Mock()
        req.get_json.return_value = {
            'blob_url': 'https://test.blob/input.parquet',
            'config': config
        }
        
        profile_response = profile_main(req)
        assert profile_response.status_code == 200
        
        profile_result = json.loads(profile_response.get_body().decode())
        assert profile_result['profile']['total_rows'] == 100
    
    with patch('clean_deterministic.download_dataframe', return_value=test_data), \
         patch('clean_deterministic.upload_dataframe', return_value='https://test.blob/cleaned.parquet'):
        
        req = Mock()
        req.get_json.return_value = {
            'blob_url': 'https://test.blob/input.parquet',
            'config': config
        }
        
        clean_response = clean_main(req)
        assert clean_response.status_code == 200
        
        clean_result = json.loads(clean_response.get_body().decode())
        assert 'cleaned_blob_url' in clean_result
        assert clean_result['stats']['input_rows'] == 100


@pytest.mark.integration
def test_classification_pipeline():
    """Test classification workflow."""
    from classify_categories import main as classify_main
    
    records = [
        {
            'record_id': 'BHP-PO-0000001',
            'description': 'Hydraulic hose assembly for Cat 789',
            'supplier_name': 'Caterpillar Inc',
            'amount': 1500.00,
            'unit': 'EA'
        },
        {
            'record_id': 'BHP-PO-0000002',
            'description': 'Bulk diesel delivery',
            'supplier_name': 'Shell Australia',
            'amount': 50000.00,
            'unit': 'L'
        }
    ]
    
    taxonomy = {
        'Equipment & Parts': {
            'Mobile Equipment': ['Haul truck parts', 'Excavator parts']
        },
        'Energy & Fuel': {
            'Diesel': ['Bulk diesel']
        }
    }
    
    req = Mock()
    req.get_json.return_value = {
        'records': records,
        'taxonomy': taxonomy,
        'examples': []
    }
    
    response = classify_main(req)
    assert response.status_code == 200
    
    result = json.loads(response.get_body().decode())
    assert 'classifications' in result
    assert len(result['classifications']) == 2
    assert result['stats']['total'] == 2


@pytest.mark.integration
def test_learning_state_update():
    """Test learning state update workflow."""
    from update_learning_state import main as update_main
    
    with patch('update_learning_state.read_sharepoint_json', return_value={}), \
         patch('update_learning_state.write_sharepoint_json') as mock_write:
        
        req = Mock()
        req.get_json.return_value = {
            'vendor_mappings': [
                {'dirty': 'CAT INC', 'canonical': 'Caterpillar Inc', 'id': 'SUP-00001'}
            ],
            'abbreviations': [
                {'abbrev': 'HYDR', 'expansion': 'hydraulic'}
            ]
        }
        
        response = update_main(req)
        assert response.status_code == 200
        
        result = json.loads(response.get_body().decode())
        assert result['updated'] == True
        assert result['new_vendor_mappings'] == 1
        assert result['new_abbreviations'] == 1
