# PySpark Code Converter

This project allows users to convert source code in various languages to PySpark using Google AI's generative models. The tool supports COBOL, Java, .NET, Python, SQL, and plaintext files and provides a user-friendly interface built with Streamlit.

## Features
- **Code Conversion:** Converts code from COBOL, Java, .NET, Python, SQL, and plaintext to PySpark.
- **Validation:** Validates the generated PySpark code for runtime and compilation errors.
- **Dynamic Input:** Accepts code files in supported formats and handles them efficiently.
- **AI Integration:** Leverages Google Generative AI (Gemini 1.5) for conversion and validation.

## Installation
1. Clone this repository:
   ```bash
   git clone https://github.com/your-repo/pyspark-code-converter.git
   cd pyspark-code-converter
   ```
2. Install the required dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Set up your Google AI API key:
   - Obtain an API key from Google Generative AI.
   - Replace `your_google_api_key` in the code with your actual API key.

4. Run the application:
   ```bash
   streamlit run main.py
   ```

## Usage
1. Start the application with the command:
   ```bash
   streamlit run main.py
   ```
2. Upload a code file in any of the supported formats (`.java`, `.cob`, `.cbl`, `.txt`, `.cs`, `.py`, `.sql`).
3. The tool identifies the source language based on the file type or content.
4. The code is converted to PySpark.
5. Validation runs on the converted PySpark code to ensure correctness.
6. Both the converted code and validation results are displayed.

## Supported Languages
- **COBOL (.cob, .cbl):** Converts COBOL constructs like file handling and loops to equivalent PySpark operations.
- **Java (.java):** Handles core Java functionality and translates into PySpark transformations.
- **.NET (.cs):** Translates .NET methods and workflows into PySpark.
- **Python (.py):** Converts Python scripts to PySpark-compatible code.
- **SQL (.sql):** Transforms SQL queries into PySpark SQL queries.
- **Plaintext (.txt):** Attempts to interpret and convert generic code snippets.

## Code Example
Below is an example workflow of conversion:

1. **Upload:** Upload a COBOL file.
2. **Conversion Prompt:** The tool generates specific prompts based on the language:
   ```plaintext
   You are an advanced AI specializing in converting COBOL source code to PySpark. Your objective is to accurately translate COBOL constructs into equivalent PySpark code while maintaining the original functionality, logic, and structure.
   ```
3. **Generated Output:**
   ```python
   from pyspark.sql import SparkSession

   spark = SparkSession.builder.appName("COBOL Conversion").getOrCreate()
   # Translated PySpark code here
   ```

4. **Validation:** Outputs validation results with dummy inputs for debugging.



