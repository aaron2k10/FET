import pandas as pd


df = pd.read_excel('C:\\Users\SARSOMAS\PycharmProjects\pythonProject\DCCLAT\DCC\media\JobResults\Job_results.xlsx')

df = df[(df['RETURN_CODE']=='CC 0000')]

df.to_excel('C:\\Users\SARSOMAS\PycharmProjects\pythonProject\DCCLAT\DCC\media\JobResults\Completed.xlsx')



