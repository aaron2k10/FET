from django.db import models

# Create your models here.

class Result(models.Model):
  Row = models.IntegerField()
  Column = models.CharField(max_length=255)
  ComparedValue = models.CharField(max_length=255)
  OriginalValue = models.CharField(max_length=255)
