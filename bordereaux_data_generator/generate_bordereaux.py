import argparse     # Parse command-line arguments to customize file
import random       # Random number and choice functions
import json
import numpy as np  # Add this for Poisson distribution
import csv          # CSV file handling
import os           # File path operations
import pandas as pd # Excel file handling
from datetime import datetime, timedelta # For generating realistic dates

def load_config(config_file_path=None):
    '''
    Load configuration from a file or use default config
    '''
    # If no config file is provided, load the default
    if config_file_path is None or not os.path.exists(config_file_path):
        default_config_path = os.path.join(os.path.dirname(__file__), 'configs', 'default_config.json')
        
        # Check if default config exists, if not create it
        if not os.path.exists(default_config_path):
            print(f"Default config not found at {default_config_path}. Using default built-in configuration.")
            return get_built_in_default_config()
        
        with open(default_config_path, 'r') as f:
            return json.load(f)
        
    # Load the provided config file
    try:
        with open(config_file_path, 'r') as f:
            config = json.load(f)
            print(f"Loaded configuration from {config_file_path}")
            return config
    except Exception as e:
        print(f"Error loading config file: {e}")
        print("Using built-in default configuration instead.")
        return get_built_in_default_config()

def get_built_in_default_config():
    '''
    Return a hardcoded default configuration in case no file is available
    '''
    return {
        "output": {
            "format": "json",
            "file_name": "claims"
        },
        "schema": {
            "fields": {
                "policy_number": {"output_name": "policy_number", "type": "string"},
                "months_insured": {"output_name": "months_insured", "type": "integer"},
                "has_claims": {"output_name": "has_claims", "type": "boolean"},
                "items_insured": {"output_name": "items_insured", "type": "integer"},
                "line_of_business": {"output_name": "line_of_business", "type": "string"},
                "state": {"output_name": "state", "type": "string"},
                "geographic_location": {"output_name": "geographic_location", "type": "string"},
                "claim_reference": {"output_name": "claim_reference", "type": "string"},
                "insured_name": {"output_name": "insured_name", "type": "string"},
                "policy_start_date": {"output_name": "policy_start_date", "type": "date", "format": "%Y-%m-%d"},
                "date_of_loss": {"output_name": "date_of_loss", "type": "date", "format": "%Y-%m-%d"},
                "date_reported": {"output_name": "date_reported", "type": "date", "format": "%Y-%m-%d"},
                "claim_status": {"output_name": "claim_status", "type": "string"},
                "claim_close_date": {"output_name": "claim_close_date", "type": "date", "format": "%Y-%m-%d"},
                "loss_type": {"output_name": "loss_type", "type": "string"},
                "paid_amount": {"output_name": "paid_amount", "type": "decimal", "precision": 2},
                "reserve_amount": {"output_name": "reserve_amount", "type": "decimal", "precision": 2},
                "total_incurred": {"output_name": "total_incurred", "type": "decimal", "precision": 2},
                "claim_propensity": {"output_name": "claim_propensity", "type": "decimal", "precision": 2}
            }
        },
        "data_variability": {
            "enabled": False,
            "column_settings": {}
        }
    }

def generate_data(num_records, config):
    '''
    Return a Dataframe of num_records simulated insurance claims.
    '''
    # Reference date is date program is run
    today = datetime.now()
    
    # First, generate a pool of policies
    num_policies = max(10, num_records // 2)
    policies = []

    # Define US states with geographic regions
    # Define US states with geographic regions
    states_by_region = {
        "Northeast": ["ME", "NH", "VT", "MA", "RI", "CT", "NY", "NJ", "PA", "DE", "MD"],
        "Southeast": ["VA", "WV", "KY", "NC", "SC", "GA", "FL", "TN", "AL", "MS", "AR", "LA"],
        "Midwest": ["OH", "MI", "IN", "IL", "WI", "MN", "IA", "MO", "ND", "SD", "NE", "KS"],
        "Southwest": ["CA","TX", "OK", "NM", "AZ"],
        "Northwest": ["MT", "ID", "WY", "CO", "UT", "NV", "WA", "OR", "AK", "HI"]
    }


    # Create flat list of all states with their regions for easy selection
    all_states = []
    for region, state_list in states_by_region.items():
        for state in state_list:
            all_states.append({"state": state, "geographic_location": region})
    
    for i in range(num_policies):
        policy_number = f"POL{1000 + i}"
        policy_start = today - timedelta(days=random.randint(1, 1095))
        months_insured = random.randint(1, 120)
        items_insured = random.randint(1, 10)
        
        # Assign claim propensity to each policy
        # Most policies have average claim frequency, but some are high or low
        claim_propensity = random.choices(
            [0.1, 0.5, 1.2],  # Low, medium, high claim frequency
            weights=[0.7, 0.2, 0.1],  # 70% low, 20% medium, 10% high
            k=1
        )[0]

        # Assign line of business with realistic market distribution
        line_of_business = random.choices(
                ["Homeowners", "Auto Insurance", "Business Insurance", "Commercial Auto", 
                "General Liability", "Workers Compensation", "Professional Liability", 
                "Renters Insurance", "Life Insurance", "Flood", "Rental Property", 
                "Umbrella/Excess Liability", "Commercial Rental Property", "Cyber Insurance", 
                "E&O", "Inland Marine Insurance", "Boat Insurance"],
            weights=[0.30, 0.22, 0.16, 0.08, 0.07, 0.06, 0.04, 0.03, 0.01, 0.02, 0.02, 
                    0.02, 0.01, 0.01, 0.01, 0.01, 0.01]
        )[0]

        # Assign geographic location and state
        location_data = random.choice(all_states)
        state = location_data["state"]
        geographic_location = location_data["geographic_location"]
        
        policies.append({
            "policy_number": policy_number,
            "policy_start": policy_start,
            "months_insured": months_insured,
            "line_of_business": line_of_business,
            "state": state,
            "geographic_location": geographic_location,
            "items_insured": items_insured,
            "claim_propensity": claim_propensity,
            "insured_name": f"Company {1000 + i}"  # Simple company name
        })
    
    # Second, generate claims based on policy propensity
    records = []
    claim_counter = 0
    
    # Define claim statuses with realistic probabilities
    claim_statuses = ["Open", "Closed", "Pending", "Reopened", "In Litigation"]
    status_weights = [0.3, 0.5, 0.1, 0.05, 0.05] # Realistic distribution

    # Define Loss Types by Line of Business with realistic distributions
    loss_types_by_lob = {
        "Homeowners": {
            "loss_types": ["Wind Damage", "Hail Damage", "Water Damage", "Fire Damage", "Theft"],
            "weights": [0.25, 0.25, 0.25, 0.15, 0.10]
        },
        "Auto Insurance": {
            "loss_types": ["Single Vehicle Collision", "Multi Vehicle Collision", "Comprehensive", "Windshield", "Pedestrian Strike", "Hit and Run"],
            "weights": [0.35, 0.30, 0.20, 0.08, 0.01, 0.04]
        },
        "Business Insurance": {
            "loss_types": ["Theft", "Property Damage", "Fire Damage", "Water Damage", "Liability Claims", "Business Interruption", "Equipment Breakdown"],
            "weights": [0.32, 0.20, 0.15, 0.12, 0.11, 0.06, 0.04]
        },
        "Commercial Auto": {
            "loss_types": ["Fleet Collision", "Commercial Vehicle Theft", "Cargo Damage"],
            "weights": [0.55, 0.25, 0.20]
        },
        "General Liability": {
            "loss_types": ["Product Liability", "Property Damage", "Personal Injury", "Advertising Injury"],
            "weights": [0.40, 0.30, 0.20, 0.10]
        },
        "Workers Compensation": {
            "loss_types": ["Workplace Injury", "Repetitive Strain"],
            "weights": [0.70, 0.30]
        },
        "Professional Liability": {
            "loss_types": ["Professional Negligence", "Breach of Contract", "Failure to Deliver", "Misrepresentation"],
            "weights": [0.50, 0.25, 0.15, 0.10]
        },
        "Renters Insurance": {
            "loss_types": ["Personal Property Theft", "Water Damage", "Fire Damage"],
            "weights": [0.50, 0.30, 0.20]
        },
        "Life Insurance": {
            "loss_types": ["Natural Death", "Accidental Death"],
            "weights": [0.90, 0.10]
        },
        "Flood": {
            "loss_types": ["Flash Flooding"],
            "weights": [1.00]
        },
        "Rental Property": {
            "loss_types": ["Tenant Damage", "Fire Damage", "Water Damage", "Theft", "Loss of Rent", "Liability Claims"],
            "weights": [0.30, 0.20, 0.18, 0.15, 0.10, 0.07]
        },
        "Umbrella/Excess Liability": {
            "loss_types": ["Premises Liability", "Personal Injury", "Property Damage"],
            "weights": [0.50, 0.30, 0.20]
        },
        "Commercial Rental Property": {
            "loss_types": ["Tenant Vandalism", "Fire Damage", "Water Damage", "Theft", "Liability Claims", "Loss of Income"],
            "weights": [0.25, 0.20, 0.18, 0.15, 0.12, 0.10]
        },
        "Cyber Insurance": {
            "loss_types": ["Data Breach", "Ransomware", "Business Interruption"],
            "weights": [0.45, 0.30, 0.25]
        },
        "E&O": {
            "loss_types": ["Professional Negligence", "Failure to Perform", "Misrepresentation", "Breach of Duty", "Inadequate Work"],
            "weights": [0.45, 0.25, 0.15, 0.10, 0.05]
        },
        "Inland Marine Insurance": {
            "loss_types": ["Equipment Theft", "Equipment Misplacement", "Accidental Damage"],
            "weights": [0.50, 0.30, 0.20]
        },
        "Boat Insurance": {
            "loss_types": ["Boat Collision", "Boat Theft", "Fire Damage"],
            "weights": [0.60, 0.25, 0.15]
        }
    }
    
    # Generate claims for each policy based on its propensity
    for policy in policies:
        # Use Poisson distribution with policy's propensity to determine number of claims
        base_rate = 0.8  # Base expected claims per policy
        adjusted_rate = base_rate * policy["claim_propensity"]
        num_policy_claims = max(0, np.random.poisson(adjusted_rate))
        
        # Generate that many claims for this policy
        for j in range(num_policy_claims):
            if claim_counter >= num_records:
                break  # Stop if we've reached the target number of records
                
            # 2. Claim reference: CLM1000, CLM1001, ...
            claim_reference = f"CLM{1000 + claim_counter}"
            claim_counter += 1
            
            # Generate claim details
            # Date of Loss: between policy start and reference date
            policy_start = policy["policy_start"]
            date_of_loss = policy_start + timedelta(days=random.randint(1, (today - policy_start).days))
            
            # Date reported: Most claims reported quickly, but some can be very delayed
            days_available = (today - date_of_loss).days
            
            # Weighted reporting delay distribution
            if days_available >= 365:
                # All delay options available
                delay_ranges = [
                    random.randint(1, 30),      # Quick reporting (1-30 days)
                    random.randint(31, 90),     # Normal delay (31-90 days)  
                    random.randint(91, 180),    # Significant delay (91-180 days)
                    random.randint(181, 365)    # Very late discovery (181-365 days)
                ]
                weights = [0.87, 0.08, 0.04, 0.01]
            elif days_available >= 180:
                # First 3 options available
                delay_ranges = [
                    random.randint(1, 30),
                    random.randint(31, 90),
                    random.randint(91, min(180, days_available))
                ]
                weights = [0.87, 0.08, 0.05]  # Redistribute the 1% into other categories
            elif days_available >= 90:
                # First 2 options available
                delay_ranges = [
                    random.randint(1, 30),
                    random.randint(31, min(90, days_available))
                ]
                weights = [0.87, 0.13]  # Redistribute into normal delay
            else:
                # Only quick reporting available
                max_delay = max(1, min(30, days_available))
                delay_ranges = [random.randint(1, max_delay)]
                weights = [1.0]
            
            reporting_delay = random.choices(delay_ranges, weights=weights, k=1)[0]
            date_reported = date_of_loss + timedelta(days=reporting_delay)
            
            # Claim status: weighted random choice
            claim_status = random.choices(claim_statuses, weights=status_weights, k=1)[0]

            # Generate claim close date for closed claims
            if claim_status == "Closed":
                # Claims can close anywhere from 7 days to 2 years after reporting
                days_to_close = random.choices(
                    [random.randint(7, 30),      # Quick resolution (7-30 days)
                    random.randint(31, 90),     # Normal resolution (1-3 months)
                    random.randint(91, 365),    # Slow resolution (3-12 months)
                    random.randint(366, 730)],  # Very slow (1-2 years)
                    weights=[0.3, 0.4, 0.25, 0.05]  # 30% quick, 40% normal, 25% slow, 5% very slow
                )[0]
                claim_close_date = date_reported + timedelta(days=days_to_close)
                # If close date would be in the future, randomly decide to close as of today, or leave it 
                # open - this prevents a bunch of future close dates all piling up on whatever day the program is ran
                if claim_close_date > today:
                    if random.random() < 0.3: # 30% chance to close today
                        claim_close_date = today
                    else: # 70% chance to remain open
                        claim_status = "Open"
                        claim_close_date = None
            else:
                claim_close_date = None  # Open claims don't have a close date
            
            # Loss type based on line of business
            lob_loss_data = loss_types_by_lob[policy["line_of_business"]]
            loss_type = random.choices(
                lob_loss_data["loss_types"], 
                weights=lob_loss_data["weights"], 
                k=1
            )[0]
            
            # Financial amounts - gamma distribution for realistic claim sizes
            total_incurred = round(random.gammavariate(2, 10000), 2)
            
            # For open claims, some will be paid and some still in reserve
            # For closed claims, all amount will be paid
            if claim_status == "Closed":
                paid_amount = total_incurred
                reserve_amount = 0
            else:
                # For open claims, random portion has been paid
                paid_ratio = random.uniform(0, 0.8) # 0-80% paid so far
                paid_amount = round(total_incurred * paid_ratio, 2)
                reserve_amount = round(total_incurred - paid_amount, 2)
            
            # Create the record
            records.append({
                # Original policy information fields
                "policy_number": policy["policy_number"],
                "months_insured": policy["months_insured"],
                "has_claims": True,  # Since we're generating claims
                "items_insured": policy["items_insured"],
                "line_of_business": policy["line_of_business"],
                "state": policy["state"],
                "geographic_location": policy["geographic_location"],
                
                # Bordereaux-specific fields
                "claim_reference": claim_reference,
                "insured_name": policy["insured_name"],
                "policy_start_date": policy_start.strftime("%Y-%m-%d"),
                "date_of_loss": date_of_loss.strftime("%Y-%m-%d"),
                "date_reported": date_reported.strftime("%Y-%m-%d"),
                "claim_status": claim_status,
                "claim_close_date": claim_close_date.strftime("%Y-%m-%d") if claim_close_date else None,
                "loss_type": loss_type,
                "paid_amount": paid_amount,
                "reserve_amount": reserve_amount,
                "total_incurred": total_incurred,
                "claim_propensity": policy["claim_propensity"]  # Include this for analysis
            })
    
    # If the run generated fewer claims than requested, add additional random claims
    # (This can happen with Poisson distribution)
    while claim_counter < num_records:
        # Pick policies weighted by their claim propensity
        # This respects that some policies are more likely to have claims
        policy = random.choices(
            policies,
            weights=[p["claim_propensity"] for p in policies],
            k=1
        )[0]
            
        # Generate a new claim using the same process as above
        claim_reference = f"CLM{1000 + claim_counter}"
        claim_counter += 1
        
        # Generate claim details
        policy_start = policy["policy_start"]
        date_of_loss = policy_start + timedelta(days=random.randint(1, (today - policy_start).days))
        days_available = (today - date_of_loss).days
        
        # Weighted reporting delay distribution
        if days_available >= 365:
            # All delay options available
            delay_ranges = [
                random.randint(1, 30),      # Quick reporting (1-30 days)
                random.randint(31, 90),     # Normal delay (31-90 days)  
                random.randint(91, 180),    # Significant delay (91-180 days)
                random.randint(181, 365)    # Very late discovery (181-365 days)
            ]
            weights = [0.87, 0.08, 0.04, 0.01]
        elif days_available >= 180:
            # First 3 options available
            delay_ranges = [
                random.randint(1, 30),
                random.randint(31, 90),
                random.randint(91, min(180, days_available))
            ]
            weights = [0.87, 0.08, 0.05]  # Redistribute the 1% into other categories
        elif days_available >= 90:
            # First 2 options available
            delay_ranges = [
                random.randint(1, 30),
                random.randint(31, min(90, days_available))
            ]
            weights = [0.87, 0.13]  # Redistribute into normal delay
        else:
            # Only quick reporting available
            max_delay = max(1, min(30, days_available))
            delay_ranges = [random.randint(1, max_delay)]
            weights = [1.0]
        
        reporting_delay = random.choices(delay_ranges, weights=weights, k=1)[0]
        date_reported = date_of_loss + timedelta(days=reporting_delay)
        claim_status = random.choices(claim_statuses, weights=status_weights, k=1)[0]
        
        # Generate claim close date for closed claims
        if claim_status == "Closed":
            days_to_close = random.choices(
                [random.randint(7, 30),
                random.randint(31, 90),
                random.randint(91, 365),
                random.randint(366, 730)],
                weights=[0.3, 0.4, 0.25, 0.05]
            )[0]
            claim_close_date = date_reported + timedelta(days=days_to_close)
            # If close date would be in the future, randomly decide to close as of today, or leave it 
            # open - this prevents a bunch of future close dates all piling up on whatever day the program is ran
            if claim_close_date > today:
                    if random.random() < 0.3: # 30% chance to close today
                        claim_close_date = today
                    else: # 70% chance to remain open
                        claim_status = "Open"
                        claim_close_date = None
        else:
            claim_close_date = None
        
        # Loss type based on line of business
        lob_loss_data = loss_types_by_lob[policy["line_of_business"]]
        loss_type = random.choices(
            lob_loss_data["loss_types"], 
            weights=lob_loss_data["weights"], 
            k=1
        )[0]

        total_incurred = round(random.gammavariate(2, 10000), 2)
        
        if claim_status == "Closed":
            paid_amount = total_incurred
            reserve_amount = 0
        else:
            paid_ratio = random.uniform(0, 0.8)
            paid_amount = round(total_incurred * paid_ratio, 2)
            reserve_amount = round(total_incurred - paid_amount, 2)
        
        records.append({
            "policy_number": policy["policy_number"],
            "months_insured": policy["months_insured"],
            "has_claims": True,
            "items_insured": policy["items_insured"],
            "line_of_business": policy["line_of_business"],
            "state": policy["state"],
            "geographic_location": policy["geographic_location"],
            "claim_reference": claim_reference,
            "insured_name": policy["insured_name"],
            "policy_start_date": policy_start.strftime("%Y-%m-%d"),
            "date_of_loss": date_of_loss.strftime("%Y-%m-%d"),
            "date_reported": date_reported.strftime("%Y-%m-%d"),
            "claim_status": claim_status,
            "claim_close_date": claim_close_date.strftime("%Y-%m-%d") if claim_close_date else None,
            "loss_type": loss_type,
            "paid_amount": paid_amount,
            "reserve_amount": reserve_amount,
            "total_incurred": total_incurred,
            "claim_propensity": policy["claim_propensity"]
        })

    # Map the records according to the schema
    mapped_records = map_records_to_schema(records, config["schema"]["fields"])

    # Apply data variability if enabled in the config
    if config.get("data_variability", {}).get("enabled", False):
        mapped_records = apply_data_variability(mapped_records,config)

    # Returns mapped_records
    return mapped_records

def apply_data_variability(records, config):
    '''
    Apply data variability settings from the config to the records
    '''
    variability_config = config["data_variability"]

    # Make a copy to avoid modifying the original
    records_copy = []

    for record in records:
        # Create a new copy of the record
        new_record = record.copy()

        # Get column-specific settings
        column_settings = variability_config.get("column_settings", {})

        # Apply variations based on column settings
        for field_name, field_value in new_record.items():
            # Check if this field has variations enabled
            field_settings = column_settings.get(field_name, {})

            if field_settings.get("variation_enabled", False):
                # Apply missing values based on probability
                missing_prob = field_settings.get("missing_value_probability", 0)
                if missing_prob > 0 and random.random() < missing_prob:
                    new_record[field_name] = "" if isinstance(field_value, str) else None
                else: # Only apply other variations if the value is not missing
                    # Apply date format variations
                    if "date_formats" in field_settings and field_value:
                        try:
                            # Attempt to parse the date using common formats
                            date_obj = None
                            common_formats = [
                                "%Y-%m-%d",      # ISO format
                                "%m/%d/%Y",      # US format  
                                "%d/%m/%Y",      # European format
                                "%d-%b-%Y",      # Month name format
                                "%Y%m%d",        # Compact format
                            ]

                            # Try each format until one works
                            for fmt in common_formats:
                                try:
                                    date_obj = datetime.strptime(field_value, fmt)
                                    break
                                except ValueError:
                                    continue

                            # If the date is parsed, apply random format
                            if date_obj:
                                new_format = random.choice(field_settings["date_formats"])
                                new_record[field_name] = date_obj.strftime(new_format)
                        except Exception:
                            pass # Keep original if parsing fails

                    # Apply currency formatting variations
                    if field_settings.get("format_with_currency", False) and field_value is not None:
                        if random.random() < 0.5: # 50% chance to add currency symbol
                            new_record[field_name] = f"${field_value}"

        records_copy.append(new_record)

    return records_copy

def map_records_to_schema(records, schema_fields):
    '''
    Map the records to the output schema defined in the config.
    
    NOTE: This function currently handles basic field name mapping, but in production scenarios
    it is possible to encounter numerous variations:
    - Policy_number, Policy_#, Policy#, Policy_num, PolicyNumber, policyno, etc.
    - Date_of_loss, LossDate, loss-dt, DateOfLoss, Incident_Date, etc.
    - Different date formats: YYYY-MM-DD, MM/DD/YYYY, DD-MON-YYYY, etc.
    
    Future enhancements to consider:
    1. Fuzzy matching for similar field names
    2. Regular expression patterns for field indentification
    3. Machine learning to detect field mappings
    4. Validation rules to ensure correct field mapping
    5. Transformation functions for complex data conversions
    
    For now, this relies on exact field name matches as define in the config.
    '''
    mapped_records = []

    for record in records:
        mapped_record = {}

        for field_name, field_config in schema_fields.items():
            if field_name in record:
                # Get the output field name from schema
                output_name = field_config["output_name"]

                # Get the value
                value = record[field_name]

                # TODO: Add more sophisticated field matching logic here
                # - Case-insensitive matching
                # - Partial string matching
                # - Synonym detection

                # Apply any field-specific transformations based on type
                if field_config["type"] == "date" and "format" in field_config and value:
                    # Only apply format if there is a value and it's a string
                    if isinstance(value, str) and not value.startswith('$'):
                        try:
                            # Try to parse with common formats
                            # TODO: Expand this list based on real TPA data patterns
                            date_formats = [
                                "%Y-%m-%d",      # 2024-01-15
                                "%m/%d/%Y",      # 01/15/2024
                                "%d-%b-%Y",      # 15-Jan-2024
                                "%d/%m/%Y",      # 15/01/2024
                                "%Y%m%d",        # 20240115
                                "%m-%d-%Y",      # 01-15-2024
                                "%d.%m.%Y",      # 15.01.2024
                                "%b %d, %Y",     # Jan 15, 2024
                                "%B %d, %Y",     # January 15, 2024
                            ]

                            for fmt in date_formats:
                                try:
                                    date_obj = datetime.strptime(value, fmt)
                                    value = date_obj.strftime(field_config["format"])
                                    break
                                except ValueError:
                                    continue
                        except Exception:
                            pass # Keep original if all parsing fails
                elif field_config["type"] == "decimal" and "precision" in field_config:
                    if value is not None and not isinstance(value, str):
                        # Format according to specified precision
                        value = round(float(value), field_config["precision"])
                    # TODO: Handle currency strings like "$1,234.56" or "1234.56 USD"

                # Add to the mapped record
                mapped_record[output_name] = value
            else:
                # Field not found in record
                # TODO: Implement fuzzy matching here
                # For now, just skip missing fields
                pass

        mapped_records.append(mapped_record)

    return mapped_records

def save_to_json(records, file_name="claims"):
    '''
    Save records to a JSON file
    '''
    output_path = os.path.join("output", f"{file_name}.json")
    with open(output_path, "w") as f:
        json.dump(records, f, indent=2)

    return output_path

def save_to_csv(records, config, file_name="claims"):
    '''
    Save records to a CSV file
    '''
    output_path = os.path.join("output", f"{file_name}.csv")

    if not records:
        print("No records to write to CSV")
        return output_path
    
    # Get field names from the first record
    field_names = list(records[0].keys())

    # Apply column order variations if enabled
    if config.get("data_variability", {}).get("enabled", False):
        column_settings = config["data_variability"].get("column_settings", {})
        # Check if any column has order variations enabled
        for settings in column_settings.values():
            if settings.get("column_order_variations", False):
                random.shuffle(field_names)
                break

    with open(output_path, "w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=field_names)
        writer.writeheader()
        writer.writerows(records)

    return output_path

def save_to_excel(records, config, file_name="claims"):
    '''
    Save records to an Excel file
    '''
    output_path = os.path.join("output", f"{file_name}.xlsx")
   
    if not records:
        print("No records to write to Excel")
        return output_path
   
    # Convert records to DataFrame
    df = pd.DataFrame(records)
   
    # Apply column order variations if enabled
    if config.get("data_variability", {}).get("enabled", False):
        column_settings = config["data_variability"].get("column_settings", {})
        # Check if any column has order variations enabled
        for settings in column_settings.values():
            if settings.get("column_order_variations", False):
                columns = list(df.columns)
                random.shuffle(columns)
                df = df[columns]
                break
   
    # Save to Excel with auto-fit columns
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        df.to_excel(writer, index=False)
        
        # Auto-fit columns
        worksheet = writer.sheets['Sheet1']
        for column in df:
            column_length = max(df[column].astype(str).map(len).max(), len(column))
            col_idx = df.columns.get_loc(column)
            worksheet.column_dimensions[worksheet.cell(row=1, column=col_idx+1).column_letter].width = column_length + 2
   
    return output_path
                    
def parse_args():
    '''
    Parse command line-line arguments
    '''
    
    parser = argparse.ArgumentParser(
        description="Generate simulated insurance bordereaux claims data"
    )

    parser.add_argument(
        "-c", "--count",
        type=int,
        default=1000,
        help="Number of records to generate (default: 1000)"
    )
    
    parser.add_argument(
        "--config",
        type=str,
        help="Path to JSON configuration file"
    )
    
    parser.add_argument(
        "-o", "--output",
        type=str,
        choices=["json", "csv", "excel"],
        help="Output format (overrides config setting if specified)"
    )
    
    parser.add_argument(
        "-f", "--filename",
        type=str,
        help="Output filename without extension (overrides config setting if specified)"
    )
    
    return parser.parse_args()

def main():
    args = parse_args()
    
    # Load configuration
    config = load_config(args.config)

    # Ensure output directory exists
    if not os.path.exists("output"):
        os.makedirs("output")
    
    # Override config settings with command line arguments if provided
    if args.output:
        config["output"]["format"] = args.output
    
    if args.filename:
        config["output"]["file_name"] = args.filename
    
    # Generate the data
    records = generate_data(args.count, config)
    
    # Save the data in the specified format
    output_format = config["output"]["format"]
    file_name = config["output"]["file_name"]
    
    if output_format == "json":
        output_path = save_to_json(records, file_name)
    elif output_format == "csv":
        output_path = save_to_csv(records, config, file_name)
    elif output_format == "excel":
        output_path = save_to_excel(records, config, file_name)
    else:
        print(f"Unsupported output format: {output_format}. Defaulting to JSON.")
        output_path = save_to_json(records, file_name)
    
    print(f"Generated {output_path} with {len(records)} records.")

if __name__ == "__main__":
    main()