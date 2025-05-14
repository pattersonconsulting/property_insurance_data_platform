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
        default_config_path = os.path.join(os.path.dirname(__file__), 'default_config.json')

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
                "claim_reference": {"output_name": "claim_reference", "type": "string"},
                "insured_name": {"output_name": "insured_name", "type": "string"},
                "policy_start_date": {"output_name": "policy_start_date", "type": "date", "format": "%Y-%m-%d"},
                "date_of_loss": {"output_name": "date_of_loss", "type": "date", "format": "%Y-%m-%d"},
                "date_reported": {"output_name": "date_reported", "type": "date", "format": "%Y-%m-%d"},
                "claim_status": {"output_name": "claim_status", "type": "string"},
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
    # End of 2025 as reference for "today"
    today = datetime(2025, 12, 31)
    
    # First, generate a pool of policies
    num_policies = max(10, num_records // 2)
    policies = []
    
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
        
        policies.append({
            "policy_number": policy_number,
            "policy_start": policy_start,
            "months_insured": months_insured,
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

    # Define Loss Types
    loss_types = ["Fire", "Water Damage", "Theft", "Liability", "Property Damage", 
                 "Business Interruption", "Natural Disaster", "Other"]
    
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
            
            # Date reported: between date of loss and reference date (usually within 30 days)
            reporting_delay = min(30, (today - date_of_loss).days)
            date_reported = date_of_loss + timedelta(days=random.randint(1, max(1, reporting_delay)))
            
            # Claim status: weighted random choice
            claim_status = random.choices(claim_statuses, weights=status_weights, k=1)[0]
            
            # Loss type
            loss_type = random.choice(loss_types)
            
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
                
                # Bordereaux-specific fields
                "claim_reference": claim_reference,
                "insured_name": policy["insured_name"],
                "policy_start_date": policy_start.strftime("%Y-%m-%d"),
                "date_of_loss": date_of_loss.strftime("%Y-%m-%d"),
                "date_reported": date_reported.strftime("%Y-%m-%d"),
                "claim_status": claim_status,
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
        
        # Generate claim details (repeat of code above)
        policy_start = policy["policy_start"]
        date_of_loss = policy_start + timedelta(days=random.randint(1, (today - policy_start).days))
        reporting_delay = min(30, (today - date_of_loss).days)
        date_reported = date_of_loss + timedelta(days=random.randint(1, max(1, reporting_delay)))
        claim_status = random.choices(claim_statuses, weights=status_weights, k=1)[0]
        loss_type = random.choice(loss_types)
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
            "claim_reference": claim_reference,
            "insured_name": policy["insured_name"],
            "policy_start_date": policy_start.strftime("%Y-%m-%d"),
            "date_of_loss": date_of_loss.strftime("%Y-%m-%d"),
            "date_reported": date_reported.strftime("%Y-%m-%d"),
            "claim_status": claim_status,
            "loss_type": loss_type,
            "paid_amount": paid_amount,
            "reserve_amount": reserve_amount,
            "total_incurred": total_incurred,
            "claim_propensity": policy["claim_propensity"]
        })

    # Map the records according to the schema
    mapped_records = map_records_to_schema(records, config["schema"]["fields"])

    # Apply data variability if enabled in the config
    if config["data_variability"]["enabled"]:
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

                            # If the date is parsed, apple random format
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

                # Apple any field-specific transformations based on type
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
    output_path = f"{file_name}.json"
    with open(output_path, "w") as f:
        json.dump(records, f, indent=2)

    return output_path

def save_to_csv(records, config, file_name="claims"):
    '''
    Save records to a CSV file
    '''
    output_path = f"{file_name}.csv"

    if not records:
        print("No records to write to CSV")
        return output_path
    
    # Get field names from the first record
    field_names = list(records[0].keys())

    # Apply column order variations if enabled
    if config["data_variability"]["enabled"]:
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
    output_path = f"{file_name}.xlsx"
    
    if not records:
        print("No records to write to Excel")
        return output_path
    
    # Convert records to DataFrame
    df = pd.DataFrame(records)
    
    # Apply column order variations if enabled
    if config["data_variability"]["enabled"]:
        column_settings = config["data_variability"].get("column_settings", {})
        # Check if any column has order variations enabled
        for settings in column_settings.values():
            if settings.get("column_order_variations", False):
                columns = list(df.columns)
                random.shuffle(columns)
                df = df[columns]
                break
    
    # Save to Excel
    df.to_excel(output_path, index=False)
    
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