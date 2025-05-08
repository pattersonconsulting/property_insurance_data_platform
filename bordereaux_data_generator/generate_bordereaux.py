import argparse     # Parse command-line arguments to customize file
import random       # Random number and choice functions
import json
import numpy as np  # Add this for Poisson distribution
from datetime import datetime, timedelta # For generating realistic dates

def generate_data(num_records):
    '''
    Return a Dataframe of num_records simulated insurance claims.
    '''
    # End of 2025 as reference for "today"
    today = datetime(2025, 12, 31)
    
    # First, generate a pool of policies (about 1/3 of the target number of claims)
    num_policies = max(1, num_records // 3)
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
    
    # If the run generated fewer claims than requested, top up with random claims
    # (This can happen with Poisson distribution)
    while claim_counter < num_records:
        # Pick a random policy that already has claims (more realistic)
        if records:
            # Get policies that already have claims
            existing_policy_numbers = [r["policy_number"] for r in records]
            policy_number = random.choice(existing_policy_numbers)
            
            # Find the corresponding policy
            policy = next(p for p in policies if p["policy_number"] == policy_number)
        else:
            # Fallback if no records yet
            policy = random.choice(policies)
            
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

    # Convert list of dicts into a pandas Dataframe
    return records

def parse_args():
    '''
    Parse command line-line arguments:
        -c / --count How many records to generate (default: 1000)
        -h / --help  Built-in help message (auto-generated)
    '''
    
    parser = argparse.ArgumentParser(
        description="Generate simulated insurance bordereaux claims data -> export to JSON"
    )

    parser.add_argument(
        "-c", "--count",        # Short & long flag forms
        type=int,               # Convert input to integer
        default=1000,           # Fallback if user omits the flag
        help="Number of records to generate (default: 1000)"
    )
    return parser.parse_args()  # Returns an object with .count attribute

def main():
    args = parse_args()             # Read in --count from the user
    records = generate_data(args.count)  # Generate that many records
    
    with open("claims.json", "w") as f:
            json.dump(records, f, indent=2)

    print(f"Generated claims.json with {len(records)} records.")  # Feedback to terminal

if __name__ == "__main__":
    main()