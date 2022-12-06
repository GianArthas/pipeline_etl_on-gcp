from fastapi import FastAPI
app = FastAPI()

@app.get("/")
def read_root():
    return {"message": "hola comunidad human"}
    
print("probando commits jaaa")