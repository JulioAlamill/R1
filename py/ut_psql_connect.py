from sqlalchemy import create_engine
import psycopg2 

engine = create_engine('postgresql+psycopg2://postgres:postgres123@localhost:5432/TestBed')

#Connect to the db
con= psycopg2.connect(
    host = "localhost"
    ,database="TestBed"
    ,user = "postgres"
    ,password = "postgres123")

