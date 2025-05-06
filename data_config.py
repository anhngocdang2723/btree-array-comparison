import random
from datetime import datetime, timedelta

# Data generation rules for each field
FIELD_RULES = {
    'Index': {
        'type': 'key',
        'description': 'Primary key'
    },
    'Customer Id': {
        'type': 'key',
        'description': 'Customer identifier'
    },
    'First Name': {
        'type': 'string',
        'prefix': 'firstname_',
        'description': 'Customer first name'
    },
    'Last Name': {
        'type': 'string',
        'prefix': 'lastname_',
        'description': 'Customer last name'
    },
    'Company': {
        'type': 'string',
        'prefix': 'company_',
        'description': 'Company name'
    },
    'City': {
        'type': 'string',
        'prefix': 'city_',
        'description': 'City name'
    },
    'Country': {
        'type': 'string',
        'prefix': 'country_',
        'description': 'Country name'
    },
    'Phone 1': {
        'type': 'phone',
        'format': '+{country_code}-{area_code}-{number}',
        'description': 'Primary phone number'
    },
    'Phone 2': {
        'type': 'phone',
        'format': '+{country_code}-{area_code}-{number}',
        'description': 'Secondary phone number'
    },
    'Email': {
        'type': 'email',
        'format': '{first_name}.{last_name}@example.com',
        'description': 'Email address'
    },
    'Subscription Date': {
        'type': 'date',
        'format': '%Y-%m-%d',
        'description': 'Subscription date'
    },
    'Website': {
        'type': 'url',
        'format': 'https://www.{company}.com',
        'description': 'Company website'
    }
}

def generate_field_value(field_name, key, existing_data=None):
    """Generate a value for a specific field based on its rules"""
    if field_name not in FIELD_RULES:
        return None
        
    rule = FIELD_RULES[field_name]
    field_type = rule['type']
    
    if field_type == 'key':
        return key
        
    elif field_type == 'string':
        prefix = rule.get('prefix', '')
        return f"{prefix}{key}"
        
    elif field_type == 'phone':
        country_code = random.randint(1, 99)
        area_code = random.randint(100, 999)
        number = random.randint(1000000, 9999999)
        return f"+{country_code}-{area_code}-{number}"
        
    elif field_type == 'email':
        if existing_data:
            first_name = existing_data.get('First Name', '').lower()
            last_name = existing_data.get('Last Name', '').lower()
            if first_name and last_name:
                return f"{first_name}.{last_name}@example.com"
        return f"user{key}@example.com"
        
    elif field_type == 'date':
        # Generate a random date within the last 5 years
        days_ago = random.randint(0, 365 * 5)
        date = datetime.now() - timedelta(days=days_ago)
        return date.strftime(rule['format'])
        
    elif field_type == 'url':
        if existing_data and 'Company' in existing_data:
            company = existing_data['Company'].lower().replace(' ', '')
            return f"https://www.{company}.com"
        return f"https://www.company{key}.com"
        
    return None

def generate_record(key, existing_data=None):
    """Generate a complete record with all fields"""
    record = {}
    for field_name in FIELD_RULES:
        record[field_name] = generate_field_value(field_name, key, existing_data)
    return record 