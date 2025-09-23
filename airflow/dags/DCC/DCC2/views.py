from django.shortcuts import render, redirect
from django.contrib.auth import authenticate, login
from django.contrib import messages
import os, subprocess,psycopg2
from django.core.files.storage import FileSystemStorage
from django.conf import settings
from django.http import JsonResponse, HttpResponse
from .models import Result
from django_serverside_datatable.views import ServerSideDatatableView
from sqlalchemy import create_engine
from airflow.api.client.local_client import Client
from dotenv import load_dotenv
import pandas as pd
import json
from datetime import datetime
import sqlite3

load_dotenv()
# Create your views here

from django.shortcuts import render, redirect

def login(request):
    if request.method == 'POST':
        username = request.POST.get('username')
        password = request.POST.get('password')
        print(username)
        if username == 'test' and password == 'test':
            request.session['user'] = username  # Simulate login
            return redirect('home')  # Redirect to home or dashboard
        else:
            return render(request, 'login.html', {'error_message': 'Invalid credentials'})

    return render(request, 'login.html')


def home(request):
    return render(request, 'index.html')


#def joballocation(request):
#    if request.method == 'POST' and 'file' in request.FILES:
#        uploaded_file = request.FILES['file']
#
#        # Save the uploaded file to the desired folder
#        fs = FileSystemStorage(location=os.path.join(settings.MEDIA_ROOT, 'uploads'))
#        filename = fs.save(uploaded_file.name, uploaded_file)
#
#        # File URL or success message to display on the page
#        success_message = 'File Uploaded successfully!'
#        file_url = fs.url(filename)
#
#        # Pass the success message and file URL back to the template
#        return render(request, "joballocation.html", {
#            'success_message': success_message,
#            'file_url': file_url,
#        })
#
#    return render(request, "joballocation.html")


# def joballocation(request):
    # if request.method == 'POST' and request.FILES['file']:
        # uploaded_file = request.FILES['file']
        # fs = FileSystemStorage()
        # filename = fs.save(uploaded_file.name, uploaded_file)
        # file_url = fs.url(filename)
# 
       # Process the Excel file (optional, depends on what you want to display)
        # df = pd.read_excel(fs.path(filename))
        # data = df.to_html()  # Convert the DataFrame to an HTML table
# 
        # context = {
            # 'success_message': 'File uploaded successfully!',
            # 'file_url': file_url,
            # 'data': data,
        # }
        # return render(request, 'joballocation.html', context)
# 
    # return render(request, 'joballocation.html')



def joballocation(request):
    if request.method =='POST' and 'dag' in request.POST:
            """
            folders=['../Scripts/Pyfiles','../Scripts/MNFfiles']
            for folder in folders:
                    # Check if the folder exists
                    if os.path.exists(folder):
                        # Iterate over all files in the folder
                        for filename in os.listdir(folder):
                            file_path = os.path.join(folder, filename)
                            try:
                                # Check if it is a file and delete it
                                if os.path.isfile(file_path):
                                    os.remove(file_path)
                                    print(f"Deleted file: {file_path}")
                            except Exception as e:
                                print(f"Failed to delete {file_path}. Reason: {e}")
                    else:
                        print(f"Folder does not exist: {folder}")
            """
            now = datetime.now()
            current_time = now.strftime("%H:%M:%S")
            c = Client(None, None)
            c.trigger_dag(dag_id='my_dag', run_id=current_time, conf={}) 
    if request.method == 'POST' and request.FILES.get('file'):
        try:
            uploaded_file = request.FILES['file']
            folder_path = '../Scripts/media' # Replace with your folder path
            fs = FileSystemStorage(location=folder_path)

            # Delete existing Excel files in the folder
            for filename in os.listdir(folder_path):
                if filename.endswith('.xlsx') or filename.endswith('.xls'):
                    file_path = os.path.join(folder_path, filename)
                    os.remove(file_path)
                    print(f'Deleted: {file_path}')

            filename = fs.save(uploaded_file.name, uploaded_file)
            file_url = fs.url(filename)

            # Process the Excel file
            df = pd.read_excel(fs.path(filename))
            data = df.to_html()  # Convert the DataFrame to an HTML table

            context = {
                'success_message': 'File uploaded successfully!',
                'file_url': file_url,
                'data': data,
            }
            return render(request, 'joballocation.html', context)
        except Exception as e:
            context = {
                'error_message': f'An error occurred: {e}',
            }
            return render(request, 'joballocation.html', context)
    return render(request, "joballocation.html")


def jobrun(request):
    output_data = None

    if request.method == 'POST':
        # Run the jobdetails.py script
        subprocess.run(['python', 'scripts/jobdetails.py'])

        # Read the generated Excel file
        db = create_engine(os.getenv("SQLITE_DB"))
        conn = db.connect()
        df = pd.read_sql("select * from \"output_analysis\"", conn)
        output_data = df.to_html(index=False)  # Convert DataFrame to HTML table
        conn.close()
        # Convert DataFrame to HTML table

    return render(request, 'jobrun.html', {'output_data': output_data})

def input_pull(request):
    input_pull_output = None
    # if request.method == 'POST':
    #     # Call the Python script for Input Pull
    #     try:
    #         result = subprocess.run(['python', 'scripts/Pullfiles 1.py'], capture_output=True, text=True)
    #         input_pull_output = result.stdout
    #     except Exception as e:
    #         input_pull_output = str(e)
    return render(request, 'jobrun.html', {'input_pull_output': input_pull_output})

def output_pull(request):
    output_pull_output = None
    # if request.method == 'POST':
    #     # Call the Python script for Output Pull
    #     try:
    #         result = subprocess.run(['python', 'scripts/pullfiles 2.py'], capture_output=True, text=True)
    #         output_pull_output = result.stdout
    #     except Exception as e:
    #         output_pull_output = str(e)

    return render(request, 'jobrun.html', {'output_pull_output': output_pull_output})
def pyscript(request):
    output_data = None
    if request.method == 'POST':
        scripts=request.POST.getlist('scripts')
    for script in scripts:
        subprocess.run(['python', f'scripts/pythonjobs/{script}'])
    filenames = os.listdir('scripts/pythonjobs')
    return render(request, 'sas.html', {'pythonsuccess':'Python scripts submitted!','filenames':filenames})
def sas(request):
    output_data = None
    db = create_engine(os.getenv("SQLITE_DB"))
    conn = db.connect()
     # Convert DataFrame to HTML table
    if request.method == 'POST':
        scripts = request.POST.getlist('scripts')
        res_rows=[]
        if request.POST.get('scripts')!=None:
            for script in scripts:
                result=subprocess.run(['python', f'scripts/pythonjobs/{script}'])
                res_rows.append({'SCRIPT':script,'RETURN_CODE':result.returncode})
            df=pd.DataFrame(res_rows)
            filenames = os.listdir('scripts/pythonjobs')
            python_data = df.to_html(index=False)
            return render(request, 'sas.html', {'python_data': python_data, 'filenames': filenames,'output_data': output_data})
        subprocess.run(['python', 'scripts/JobRun Status 1.py'])
        df = pd.read_sql("select * from \"jobrunstatus\"", conn)
        output_data = df.to_html(index=False)
        conn.close()
    filenames = os.listdir('scripts/pythonjobs')
    return render(request, 'sas.html', {'output_data': output_data,'filenames':filenames})

def completedjobs(request):
    output_data = None
    # if request.method == 'POST':
    #     # Run the complete.py script
    #         subprocess.run(['python3', 'scripts/complete.py'])
    #
    #     # read the generated excel sheet
    db = create_engine(os.getenv("SQLITE_DB"))
    conn = db.connect()
    df = pd.read_sql("select * from \"jobrunstatus\"", conn)
    output_data = df.to_html(index=False)
    conn.close()
    #excel_file_path = 'media/JobResults/Completed.xlsx'
    #df = pd.read_excel(excel_file_path)
    #output_data = df.to_html(index=False) # convert Dataframe to HTML table

    return render(request, 'completedjobs.html', {'output_data': output_data})

def comparereports(request):
    subprocess.run(['python', 'scripts/Compare.py'])
    db = create_engine(os.getenv("SQLITE_DB"))
    conn = db.connect()
    results=pd.read_sql_table('filelist',con=db)
    filelist=results.to_dict(orient='records')
    conn.close()
    return render(request, 'comparereports.html', {'filelist':filelist})

class ItemListView(ServerSideDatatableView):
    queryset = Result.objects.all()
    columns = ['Row', 'Column', 'ComparedValue','OriginalValue']

def test(request,tablename):
        # connection = psycopg2.connect(user="postgres",
        #                               password="admin",
        #                               host="127.0.0.1",
        #                               port="5432",
        #                               database="mydb")
        # cursor = connection.cursor()
        # query = f'SELECT * FROM "{tablename}";'
        # cursor.execute(query)
        # records = cursor.fetchall()
        # cursor.close()
        # connection.close()
        # df = pd.DataFrame(records)
        # json_str = df.to_json(orient="records")
        # #print(json_str)
        # return JsonResponse(records, safe=False)
       db = create_engine(os.getenv("SQLITE_DB"))
    
    # Create a cursor
       conn = db.connect()
       df = pd.read_sql(f'SELECT * FROM "{tablename}";', conn)
    #    records = list(df.to_records(index=False))
    # Execute query
    #    query = f'SELECT * FROM "{tablename}";'
    #    cursor.execute(query)
       print(f"df vararathu : {df}")
       
    # Fetch all records
    #     records = df.fetchall()
    
    # Close cursor and connection
       conn.close()
    #    connection.close()
    
    # Convert to DataFrame
    #    df = pd.DataFrame(records)
    
    # Convert to JSON
    #    json_str = df.to_json(orient="records")
    #    print(f"json string : {json_str}")
       
    #    records = [
    #     (item[0], item[1], item[2], item[3])
    #     for item in json_str]

    #    print(f"Records : {records}")
    # # Return as JsonResponse
       records = list(df.itertuples(index=False, name=None))

       print(f"list of tuples expected: {records}")
       return JsonResponse(records, safe=False)

def about(request):
    return render(request,"about.html")
