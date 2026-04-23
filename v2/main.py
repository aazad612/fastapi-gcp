import os
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse # Add this import
from gcs_service import get_and_validate_payroll

app = FastAPI(title="GCS Payroll POC")

from gcs_service import get_and_validate_payroll, get_enriched_payroll, export_payroll_to_avro

@app.get("/", response_class=HTMLResponse)
def root():
    return """
    <html>
        <body>
            <h1>GCS Payroll Pipeline POC</h1>
            <ul>
                <li><a href="/validate-payroll">1. Validate ASCII</a></li>
                <li><a href="/process-payroll">2. View Enriched Payroll Data</a></li>
                <li><a href="/export-avro">3. Export Payroll data to GCS (Avro)</a></li>
            </ul>
        </body>
    </html>
    """


@app.get("/export-avro")
def trigger_avro_export():
    try:
        # Call the service function
        gcs_path = export_payroll_to_avro()
        
        return {
            "status": "success",
            "location": gcs_path
        }
    except Exception as e:
        # This catches GCS auth issues, schema mismatches, etc.
        raise HTTPException(status_code=500, detail=str(e))
    

@app.get("/validate-payroll")
def validate_payroll():
    try:
        results = get_and_validate_payroll()
        if not results:
            return {"status": "error", "message": "No CSV files found."}
        return {"validation_results": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ... (keep existing imports)
from gcs_service import get_and_validate_payroll, get_enriched_payroll


@app.get("/process-payroll")
def process_payroll():
    try:
        data = get_enriched_payroll()
        return {
            "count": len(data),
            "records": data
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)