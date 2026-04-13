"""Update learning state function - updates blob storage dictionaries and instructions."""
import logging
import json
import azure.functions as func
from shared.sharepoint_helpers import (
    read_config_json,
    write_config_json,
    read_config_text,
    write_config_text
)


def main(req: func.HttpRequest) -> func.HttpResponse:
    """Update learning state endpoint."""
    logging.info('Update learning state function triggered')
    
    try:
        req_body = req.get_json()
    except ValueError:
        return func.HttpResponse(
            json.dumps({"error": "Invalid JSON in request body"}),
            status_code=400,
            mimetype="application/json"
        )
    
    vendor_mappings = req_body.get('vendor_mappings', [])
    abbreviations = req_body.get('abbreviations', [])
    classification_examples = req_body.get('classification_examples', [])
    instructions_append = req_body.get('instructions_append', '')
    
    try:
        new_vendor_mappings = 0
        new_abbreviations = 0
        new_examples = 0
        instructions_updated = False
        
        if vendor_mappings:
            try:
                vendor_dict = read_config_json('vendor_dictionary.json')
            except Exception:
                vendor_dict = {}
            
            for mapping in vendor_mappings:
                dirty = mapping.get('dirty')
                canonical = mapping.get('canonical')
                
                if dirty and canonical and dirty not in vendor_dict:
                    vendor_dict[dirty] = canonical
                    new_vendor_mappings += 1
            
            if new_vendor_mappings > 0:
                write_config_json('vendor_dictionary.json', vendor_dict)
                logging.info(f"Added {new_vendor_mappings} new vendor mappings")
        
        if abbreviations:
            try:
                abbrev_dict = read_config_json('abbreviation_dictionary.json')
            except Exception:
                abbrev_dict = {}
            
            for abbrev_entry in abbreviations:
                abbrev = abbrev_entry.get('abbrev')
                expansion = abbrev_entry.get('expansion')
                
                if abbrev and expansion and abbrev not in abbrev_dict:
                    abbrev_dict[abbrev] = expansion
                    new_abbreviations += 1
            
            if new_abbreviations > 0:
                write_config_json('abbreviation_dictionary.json', abbrev_dict)
                logging.info(f"Added {new_abbreviations} new abbreviations")
        
        if classification_examples:
            try:
                examples_list = read_config_json('few_shot_examples.json')
                if not isinstance(examples_list, list):
                    examples_list = []
            except Exception:
                examples_list = []
            
            existing_descriptions = {ex.get('description') for ex in examples_list}
            
            for example in classification_examples:
                description = example.get('description')
                
                if description and description not in existing_descriptions:
                    examples_list.append({
                        'description': description,
                        'l1': example.get('l1'),
                        'l2': example.get('l2'),
                        'l3': example.get('l3'),
                        'verified': example.get('verified', True)
                    })
                    new_examples += 1
                    existing_descriptions.add(description)
            
            if new_examples > 0:
                write_config_json('few_shot_examples.json', examples_list)
                logging.info(f"Added {new_examples} new classification examples")
        
        if instructions_append:
            try:
                instructions = read_config_text('agent_instructions.md')
            except Exception:
                instructions = """# Data Cleaning Agent Instructions

## Your Role
You are a procurement data cleaning agent. You orchestrate a pipeline that cleans messy procurement records into standardised, categorised data.

## Process
1. When the user provides a file, upload it and call the profile-data function
2. Review the profile results. Decide which fields need deterministic cleaning vs LLM reasoning.
3. Call clean-deterministic to process the bulk of records
4. Review the flagged records. For each batch of ~20 flagged records, classify them using the taxonomy and examples below.
5. Call validate-output to check the final data
6. If validation passes, call format-output and provide the download link
7. Call update-learning-state with any new vendor mappings or abbreviation expansions you discovered

## Learned Rules
"""
            
            if '## Learned Rules' not in instructions:
                instructions += '\n\n## Learned Rules\n'
            
            instructions += f'\n- {instructions_append}\n'
            
            write_config_text('agent_instructions.md', instructions)
            instructions_updated = True
            logging.info("Updated agent instructions")
        
        try:
            vendor_dict = read_config_json('vendor_dictionary.json')
            total_vendor_dictionary_size = len(vendor_dict)
        except Exception:
            total_vendor_dictionary_size = 0
        
        try:
            abbrev_dict = read_config_json('abbreviation_dictionary.json')
            total_abbreviation_dictionary_size = len(abbrev_dict)
        except Exception:
            total_abbreviation_dictionary_size = 0
        
        try:
            examples_list = read_config_json('few_shot_examples.json')
            total_examples_size = len(examples_list) if isinstance(examples_list, list) else 0
        except Exception:
            total_examples_size = 0
        
        return func.HttpResponse(
            json.dumps({
                "updated": True,
                "new_vendor_mappings": new_vendor_mappings,
                "new_abbreviations": new_abbreviations,
                "new_examples": new_examples,
                "instructions_updated": instructions_updated,
                "total_vendor_dictionary_size": total_vendor_dictionary_size,
                "total_abbreviation_dictionary_size": total_abbreviation_dictionary_size,
                "total_examples_size": total_examples_size
            }),
            status_code=200,
            mimetype="application/json"
        )
        
    except Exception as e:
        logging.error(f"Error updating learning state: {str(e)}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500,
            mimetype="application/json"
        )
