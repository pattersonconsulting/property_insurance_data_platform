# Bordereaux Data Generator

A Python tool that generates realistic synthetic insurance bordereaux data in JSON format, useful for development, testing, and demonstration.

![Insurance Data](https://img.shields.io/badge/Insurance-Data-blue)
![Python](https://img.shields.io/badge/Python-3.8+-green)
![License](https://img.shields.io/badge/License-MIT-yellow)

## Why This Matters

Insurance companies process thousands of bordereaux reports - detailed listings of premiums, claims, and risks that flow between insurers, brokers, and MGAs.

- **High Volume & Multiple Formats**: Insurers receive bordereaux in various formats and structures
- **Data Quality Challenges**: Manual processing is time-consuming and error-prone 
- **Development Need**: Synthetic data is essential for designing ETL pipelines and analytics
- **Testing Requirement**: Realistic test data helps validate data transformations and reports

## Features

- **Realistic Claim Modeling**: Models insurance claim propensity (some policies have more claims)
- **Statistical Distributions**: Financial values follow realistic insurance patterns
- **Date Intelligence**: Maintains logical relationships between dates (policy → loss → reporting)
- **Industry Standards**: Generates data matching insurance industry patterns and terminology
- **Configurable Volume**: Easily generate any number of records with `--count` parameter

## Coming Soon

- More bordereaux types (premium, risk)
- Enhanced demographic data (name, age, gender, zip_code)
- Schema-driven templates
- Excel output formats
- Data messiness simulation (missing values, inconsistent formats)

## Sample Output

```json
{
  "policy_number": "POL1038",
  "months_insured": 113,
  "has_claims": true,
  "items_insured": 4,
  "claim_reference": "CLM1071",
  "insured_name": "Company 1038",
  "policy_start_date": "2023-10-02",
  "date_of_loss": "2024-01-16",
  "date_reported": "2024-02-15",
  "claim_status": "Open",
  "loss_type": "Property Damage",
  "paid_amount": 11463.65,
  "reserve_amount": 11962.45,
  "total_incurred": 23426.10,
  "claim_propensity": 0.5
}
```

## Getting Started
### 1. Clone the repository:
```bash
  git clone https://github.com/pattersonconsulting/property_insurance_data_platform.git
  cd property_insurance_data_platform/bordereaux_data_generator
```

### 2. Set up a virtual environment (optional but recommended):
```bash
  # Create the virtual environment
  python -m venv venv

  # Activate it (Windows PowerShell)
  .\venv\Scripts\Activate.ps1

  # Activate it (macOS/Linux)
  source venv/bin/activate
```

### 3. Install dependencies:
```bash
  pip install -r requirements.txt
```

### 4. Generate bordereaux data:
```bash
  # Generate 1000 records (default)
  python generate_bordereaux.py

  # Or specify a custom count
  python generate_bordereaux.py --count 500
```

### 5. Review the output: The script will create a claims.json file with the generated records.

### How It Works
The generator creates a pool of insurance policies and assigns each a "claim propensity" factor that influences how likely it is to generate claims. This models the real-world pattern where certain policies tend to have more claims than others.

Financial values follow statistical distributions typical in insurance data (using gamma distributions for claim amounts). Date fields maintain logical relationships (policy inception → date of loss → reporting date).

### License
This project is licensed under the MIT License - see the LICENSE file for details.
 ---
*This tool is part of the Property Insurance Data Platform project by [Patterson Consulting](https://github.com/pattersonconsulting)*
