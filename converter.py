import os
import streamlit as st
import google.generativeai as genai

# Configure the Google AI API
genai.configure(api_key="AIzaSyDhCQondxMPWo6A4ZfkMJD6iNzvPC3M3_w")

model = genai.GenerativeModel(
    model_name="gemini-1.5-flash",
)

def convert_code(input_code, source_language):
    """
    Convert code from the specified source language to PySpark.
    """
    # Start a chat session with the generative model
    chat_session = model.start_chat(history=[])

    # Create specific prompts based on the source language
    if source_language == "Java":
        prompt = f"""
        You are an advanced AI specializing in code conversion. Your task is to convert Java source files to PySpark.
        Here is the source file data for conversion:
        {input_code}
        """
    elif source_language == "COBOL":
        prompt = f"""
        You are an advanced AI specializing in converting COBOL source code to PySpark. Your objective is to accurately translate COBOL constructs into equivalent PySpark code while maintaining the original functionality, logic, and structure. 

        *Guidelines:*
        1. *General Conversion Rules:*
           - Convert COBOL data types (e.g., PIC clauses) to appropriate PySpark data types (e.g., IntegerType, StringType).
           - Maintain the structure of the COBOL program (e.g., IDENTIFICATION DIVISION, DATA DIVISION, PROCEDURE DIVISION) in a way that reflects how it would be structured in PySpark.
        2. *Data Handling:*
           - *File Operations:* For COBOL file reads and writes (e.g., OPEN, READ, WRITE, CLOSE), convert these operations to PySpark DataFrame read/write methods. Ensure to specify the correct file formats (e.g., CSV, Parquet) based on the COBOL file handling.
           - *CSV Processing:* If the COBOL code processes CSV files, use PySpark's spark.read.csv() for reading and DataFrame.write.csv() for writing. Include any transformations that were performed on the data in COBOL.
        3. *Database Interactions:*
           - For COBOL database operations (e.g., EXEC SQL statements), convert these to PySpark SQL operations using spark.sql() or DataFrame methods. Ensure proper handling of SQL queries, connections, and data retrieval.
        4. *Complex Logic and Structures:*
           - Handle complex COBOL constructs such as loops (e.g., PERFORM, DO, END-PERFORM), conditionals (e.g., IF, ELSE, EVALUATE), and subprogram calls. Convert these to their PySpark equivalents, preserving the logic and flow.
           - If there are tables or arrays in COBOL, represent them using appropriate PySpark structures (e.g., DataFrames or Lists).
        5. *SQL Queries:*
           - If the COBOL code includes SQL queries, convert them into PySpark SQL queries while ensuring they are optimized for performance. Maintain the logic of any joins, filters, and aggregations.
        6. *Error Handling:*
           - Convert COBOL error handling (e.g., INVALID KEY, NOT ON FILE) to PySpark error handling practices, providing equivalent checks or exception handling.
        7. *Output Format:*
           - The output should be valid PySpark code that can be run in a PySpark environment without errors. Ensure correct indentation and coding conventions.

        Here is the COBOL source code that needs to be converted:
        {input_code}
        """
    elif source_language == ".NET":
        prompt = f"""
        You are an advanced AI specializing in code conversion. Your task is to convert .NET source files to PySpark.
        Here is the source file data for conversion:
        {input_code}
        """
    elif source_language == "Python":
        prompt = f"""
        You are an advanced AI specializing in code conversion. Your task is to convert Python source files to PySpark.
        Here is the source file data for conversion:
        {input_code}
        """
    elif source_language == "SQL":
        prompt = f"""
        You are an advanced AI specializing in code conversion. Your task is to convert SQL source files to PySpark SQL.
        Here is the source file data for conversion:
        {input_code}
        """
    else:
        raise ValueError("Unsupported source language.")

    response = chat_session.send_message(prompt)

    # Access the generated text from the response
    response_text = response.text.strip()

    # Extract the code snippet
    if "" in response_text and "python" in response_text:
        # Split the response by the  marker
        parsed_text = response_text.split("")[1].strip()  # Get the first code block
        # Remove the language identifier if present
        if parsed_text.startswith("python"):
            parsed_text = parsed_text[len("python"):].strip()  # Remove 'python' from the start
        return parsed_text

def validate_code(pyspark_code):
    """
    Validate the PySpark code for compilation and runtime errors using dummy input values.
    """
    validation_prompt = f"""
    You are a code validation AI. Your task is to check the following PySpark code for compilation and runtime errors.
    
    Please provide sample input data or a CSV file that would simulate runtime execution. 
    You can create sample data based on common structures in PySpark (e.g., DataFrames) or suggest necessary dummy inputs.

    Here is the PySpark code to validate:
    {pyspark_code}

    Provide dummy input values and indicate any potential compilation or runtime errors.
    """
    
    chat_session = model.start_chat(history=[])
    response = chat_session.send_message(validation_prompt)

    # Access the validation response
    if "" in response.text and "python" in response.text:
        # Split the response by the  marker
        parsed_text = response.text.split("")[1].strip()  # Get the first code block
        # Remove the language identifier if present
        if parsed_text.startswith("python"):
            parsed_text = parsed_text[len("python"):].strip()  # Remove 'python' from the start
        return parsed_text

# Streamlit UI
st.title("PySpark Code Converter")

# File upload
uploaded_file = st.file_uploader("Upload your code file", type=["java", "cob", "cbl", "txt", "cs", "py", "sql"])

if uploaded_file is not None:
    # Read the uploaded file
    input_code = uploaded_file.read().decode("utf-8")
    
    # Get the source language from the file type
    file_extension = uploaded_file.name.split('.')[-1].lower()
    source_language = {
        "java": "Java",
        "cob": "COBOL",
        "cbl": "COBOL",
        "txt": "Plaintext",
        "cs": ".NET",
        "py": "Python",
        "sql": "Teradata SQL" if "teradata" in input_code.lower() else "SQL"
    }.get(file_extension)

    if source_language:
        max_retries = 3
        # Change the validation loop to log the output
    for attempt in range(max_retries):
        # Convert the code
        converted_code = convert_code(input_code, source_language)
        
        # Validate the converted code
        validation_output = validate_code(converted_code)
        
        # Log validation output for debugging
        st.write(f"Validation Output for Attempt {attempt + 1}:")
        st.code(validation_output)

        # Check for errors in the validation output
        if "error" not in validation_output.lower():
            # Show the converted code and validation output
            st.subheader("Converted Code:")
            st.code(converted_code, language='python')
            st.subheader("Validation Output:")
            st.code(validation_output)
            break
        else:
            st.warning(f"Attempt {attempt + 1}: Validation detected an error. Retrying...")
