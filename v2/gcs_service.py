from google.cloud import storage
from toolz.curried import pipe, map, filter
import csv
import io

client = storage.Client()
BUCKET_NAME = "fastapi-cloudrun-poc"
PREFIX = "payroll/"

def is_ascii(text: str) -> bool:
    return all(ord(c) < 128 for c in text)

def validate_blob_content(blob):
    try:
        content = blob.download_as_text()
        valid = is_ascii(content)
        return {
            "file": blob.name.split('/')[-1], 
            "valid": valid,
            "status": "clean" if valid else "non-ascii detected"
        }
    except Exception as e:
        return {"file": blob.name, "valid": False, "error": str(e)}

def get_and_validate_payroll():
    blobs = list(client.list_blobs(BUCKET_NAME, prefix=PREFIX))
    
    return pipe(
        blobs,
        filter(lambda b: b.name.endswith('.csv')),
        map(validate_blob_content),
        list
    )


def parse_csv(content: str):
    """Converts raw CSV text into a list of dictionaries."""
    f = io.StringIO(content)
    return list(csv.DictReader(f))

def join_dept_name(depts_lookup, employee):
    """Enriches an employee record with the department name."""
    dept_id = employee.get("dept_id")
    employee["dept_name"] = depts_lookup.get(dept_id, "Unknown")
    return employee

def get_enriched_payroll():
    # 1. Load the Department Lookup
    dept_blob = client.bucket(BUCKET_NAME).blob(f"{PREFIX}departments.csv")
    dept_data = parse_csv(dept_blob.download_as_text())
    # Create a dictionary for O(1) lookup: {'MKT': 'Marketing', ...}
    depts_lookup = {d['dept_id']: d['dept_name'] for d in dept_data}

    # 2. Define the files to process
    payroll_files = ["marketing.csv", "HR.csv", "finance.csv"]
    
    all_employees = []
    for file_name in payroll_files:
        blob = client.bucket(BUCKET_NAME).blob(f"{PREFIX}{file_name}")
        raw_data = parse_csv(blob.download_as_text())
        
        # Use pipe to enrich each record
        enriched = pipe(
            raw_data,
            map(lambda emp: join_dept_name(depts_lookup, emp)),
            list
        )
        all_employees.extend(enriched)

    return all_employees

def export_payroll_to_avro():

    from fastavro import writer, parse_schema

    PAYROLL_SCHEMA = {
        'doc': 'Payroll records',
        'name': 'Payroll',
        'namespace': 'com.poc.payroll',
        'type': 'record',
        'fields': [
            {'name': 'emp_id', 'type': 'string'},
            {'name': 'emp_name', 'type': 'string'},
            {'name': 'dept_id', 'type': 'string'},
            {'name': 'salary', 'type': 'string'},
            {'name': 'dept_name', 'type': 'string'},
        ],
    }
    parsed_schema = parse_schema(PAYROLL_SCHEMA)

    # 1. Get the data using your existing logic
    all_employees = get_enriched_payroll()
    
    # 2. Setup the GCS destination
    output_filename = "payroll.avro"
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(f"processed/{output_filename}")
    
    # 3. Write to Avro buffer
    fo = io.BytesIO()
    writer(fo, parsed_schema, all_employees)
    
    # 4. Upload
    blob.upload_from_string(fo.getvalue(), content_type="application/octet-stream")
    
    return f"gs://{BUCKET_NAME}/processed/{output_filename}"

