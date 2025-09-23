from django.urls import path
from . import views

urlpatterns = [
    path('', views.login, name='login'),
    path('home/', views.home, name='home'),
    path('joballocation/', views.joballocation, name='joballocation'),
    path('jobrun/', views.jobrun, name='jobrun'),
    path('jobrun/input_pull/', views.input_pull, name='input_pull'),
    path('jobrun/output_pull/', views.output_pull, name='output_pull'),
    path('sas/', views.sas, name='sas'),
    path('pyscripts/', views.pyscript, name='pyscripts'),
    path('completedjobs/', views.completedjobs, name='completedjobs'),
    path('comparereports/', views.comparereports, name='comparereports'),
    path('test/<str:tablename>/', views.test, name='test'),
    path('about/', views.about, name='about'),
    path('data/', views.ItemListView.as_view(), name='data'),
]
