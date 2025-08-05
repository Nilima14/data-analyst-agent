
from fastapi import FastAPI, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from typing import List

# Import your handler functions
from handlers import handle_scraping, handle_csv_analysis, handle_high_court_analysis

# Create the FastAPI app instance
app = FastAPI(title="Data Analyst Agent")

# CORS middleware to allow cross-origin requests (use specific origins in production)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Change to specific domains in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Root endpoint
@app.get("/")
def root():
    return {"message": "Welcome to the Data Analyst Agent API"}

# Endpoint for Wikipedia scraping analysis
@app.post("/analyze/scraping")
async def analyze_scraping(questions: List[str] = Form(...)):
    result = handle_scraping(questions)
    return result

# Endpoint for CSV file analysis
@app.post("/analyze/csv")
async def analyze_csv(questions: List[str] = Form(...), file: UploadFile = File(...)):
    result = handle_csv_analysis(questions, file)
    return result

# Endpoint for High Court judgment analysis
@app.post("/analyze/highcourt")
async def analyze_highcourt(questions: List[str] = Form(...)):
    result = handle_high_court_analysis(questions)
    return result
