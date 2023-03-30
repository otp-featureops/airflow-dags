from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.models import Variable
import openai
import os

with DAG(
    'sas2python',
    description='Convert SAS to Python DAG',
    schedule='@hourly',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['featureops'],
) as dag:
    
    sas_dir = "/opt/airflow/dags/sas2py/sas_code"
    py_dir = "/tmp/py_code"
    sas_output_dir = "/tmp/sas_output"
    py_output_dir = "/tmp/py_output"
        
    def convert_sas_to_python(**context): 
        openai.api_key = Variable.get("OPENAI_API_KEY")
        
        if not os.path.exists(py_dir):
            os.makedirs(py_dir)
    
        for file in os.listdir(sas_dir):
            if file.endswith(".sas"):
                sas_file = os.path.join(sas_dir, file)
                with open(sas_file, 'r') as f:
                    sas_code = f.read()
                # Call GPT-3 to convert SAS code to Python code
                response = openai.Completion.create(
                    engine="davinci-codex",
                    prompt=sas_code,
                    max_tokens=1024,
                    n=1,
                    stop=None,
                    temperature=0.5,
                )
                python_code = response.choices[0].text
                # Save the converted code to a new file with the same name in the new directory
                new_file = os.path.join(py_dir, file.replace(".sas", ".py"))
                with open(new_file, 'w') as f:
                    f.write(python_code)

    
    def run_sas_file(sas_file_path, output_file_path):
    # Run the SAS code in the file and save the output to the output file
    # You can use the SASpy package to run the SAS code
    # For example:
        import saspy
        sas = saspy.SASsession()
        sas.submit(open(sas_file_path, 'r').read())
        sas.log(locals())
        sas_output = sas.odsout('LISTING').get_dataframe()
        sas_output.to_csv(output_file_path)
    
    
    def process_sas_files(**context):    
        if not os.path.exists(sas_output_dir):
            os.makedirs(sas_output_dir)
        for file in os.listdir(sas_dir):
            if file.endswith(".sas"):
                sas_file_path = os.path.join(sas_dir, file)
                output_file_path = os.path.join(sas_output_dir, file.replace(".sas", ".csv"))
                run_sas_file(sas_file_path, output_file_path)
    
    
    def process_py_files(filename):
        from io import StringIO
        import sys
        
        input_file = f"{py_dir}/{filename}"
        output_file = f"{py_output_dir}/{filename}"
        
        tmp = sys.stdout
        result = StringIO()
        sys.stdout = result
        
        with open(input_file, 'r') as f:
            code = f.read()  
        try:
            output = eval(code)
        except Exception as e:
            output = f"Error occurred: {e}"
            sys.stdout = tmp
            with open(output_file, 'w') as f:
                f.write(output)
            return
            
        sys.stdout = tmp
        
        with open(output_file, 'w') as f:
            f.write(result.getvalue())
    
    
    
    convert_sas_code = PythonOperator(
        task_id='convert_sas_code',
        python_callable=convert_sas_to_python,
        provide_context=True,
    )
    
    
    run_sas_files = PythonOperator(
        task_id='run_sas_files',
        provide_context=True,
        python_callable=process_sas_files,
    )
    
    run_py_files = PythonOperator(
        task_id='run_py_files',
        python_callable=lambda: [process_py_files(filename) for filename in os.listdir(py_dir) if filename.endswith('.py')],
    )
    
    
    compare_results = BashOperator(
        task_id='compare_results',
        bash_command='Compare results of sas code and python code',
    )
    
    
    
    convert_sas_code >> [run_sas_files, run_py_files] >> compare_results
    
    #run_py_files >> compare_results
    
    
  

    
    