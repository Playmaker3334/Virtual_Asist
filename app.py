from flask import Flask, render_template, request, jsonify
import os
import sys
import traceback
import pandas as pd

# Add the project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import from your existing modules
from chatbot import create_rolplay_analyzer, generate_response
from core.query_processor import process_query
from core.config import FACT_FILE_PATH, STORAGE_PATH

app = Flask(__name__, static_folder='static', template_folder='templates')

# Initialize the RAG engine
try:
    df, rag_engine = create_rolplay_analyzer(FACT_FILE_PATH)
    if df is None or rag_engine is None:
        print("Error initializing RAG engine")
except Exception as e:
    print(f"Error initializing app: {str(e)}")
    traceback.print_exc()

@app.route('/')
def index():
    """Render the main dashboard page with chat interface"""
    return render_template('index.html')

@app.route('/query', methods=['POST'])
def query():
    """Process user queries and return responses"""
    try:
        user_query = request.json.get('query', '')
        if not user_query:
            return jsonify({"error": "Query is required"}), 400
            
        # Process the query using your existing backend
        response = process_query(rag_engine, user_query, generate_response)
        
        return jsonify({
            "response": response
        })
    except Exception as e:
        print(f"Error processing query: {str(e)}")
        traceback.print_exc()
        return jsonify({"error": f"Error processing query: {str(e)}"}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)