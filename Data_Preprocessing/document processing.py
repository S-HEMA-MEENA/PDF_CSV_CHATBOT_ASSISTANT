import fitz         #extract text,image by PyMuPDF
import pdfplumber   #extract text,tables
import os
from docx import Document   #extract document
import pandas as pd
from concurrent.futures import ThreadPoolExecutor #Multithreading for faster processing
from PIL import Image   #pillow
import io
import json

UPLOAD_FOLDER="Data_Folder"
OUTPUT_FOLDER="Processed_Data"

#check floder exist
os.makedirs(OUTPUT_FOLDER,exist_ok=True)

def process_pdf(pdf_path):
    text_data,table_data,image_data=[],[],[]
    
    #using pdfplumber extracting text and tables
    with pdfplumber.open(pdf_path) as pdf:
        for page_num,page in enumerate(pdf.pages,start=1):
            text=page.extract_text()
            
            if text:
                text_data.append({"page":page_num,"text":text})
            
            tables=page.extract_tables()
            for table in tables:
                table_data.append({"page":page_num,"table":table})
                
    
    #using PyMuPDF extracting images
    pdf_doc=fitz.open(pdf_path)
    
    for page_num in range(len(pdf_doc)):
        for img_index,img in enumerate(pdf_doc[page_num].get_images(full=True)):
            xref=img[0]
            base_image=pdf_doc.extract_image(xref)
            image_bytes=base_image["image"]
            img_filename=f"{os.path.basename(pdf_path)}_page{page_num+1}_img{img_index+1}.png"
            img_path=os.path.join(OUTPUT_FOLDER,img_filename)
            
            with open(img_path,"wb") as img_file:
                img_file.write(image_bytes)
            
            image_data.append({"page":page_num+1,"image_path":img_path})
    return {"file":pdf_path,"text":text_data,"tables":table_data,"images":image_data}

def process_docx(docx_path):
    doc=Document(docx_path)
    text_data,table_data,image_data=[],[],[]
    
    #extract text
    for para in doc.paragraphs:
        text_data.append(para.text)
    
    #extract tables
    for table in doc.tables:
        table_content=[]
        for row in table.rows:
            table_content.append([cell.text for cell in row.cells])
        table_data.append(table_content)
        
    #extract images
    for rel in doc.part.rels:
        if "image" in doc.part.rels[rel].target_ref:
            img_data = doc.part.rels[rel].target_part.blob
            img_filename = f"{os.path.basename(docx_path)}_img.png"
            img_path = os.path.join(OUTPUT_FOLDER, img_filename)
            
            with open(img_path, "wb") as img_file:
                img_file.write(img_data)
            image_data.append({"image_path": img_path})

    return {"file": docx_path, "text": text_data, "tables": table_data, "images": image_data}

def process_csv(csv_path):
    df = pd.read_csv(csv_path)
    table_data = df.to_dict(orient="records")
    return {"file": csv_path, "tables": table_data}
    
def process_all_files(folder_path):
    results=[]
    files=os.listdir(folder_path)
    
    #using threadpoolexecuter for parallel processing 
    with ThreadPoolExecutor() as executor:
        futures=[]
        for file in files:
            file_path=os.path.join(folder_path,file)
            if file.endswith(".pdf"):
                futures.append(executor.submit(process_pdf,file_path))
                
            elif file.endswith(".docx"):
                futures.append(executor.submit(process_docx,file_path))
            elif file.endswith(".csv"):
                futures.append(executor.submit(process_csv,file_path))

        for future in futures:
            results.append(future.result())

    # Save extracted data as JSON
    with open(os.path.join(OUTPUT_FOLDER, "processed_data.json"), "w") as json_file:
        json.dump(results, json_file, indent=4)

    return results
    
all_data=process_all_files(UPLOAD_FOLDER)

# Run the batch processing
all_data = process_all_files(UPLOAD_FOLDER)
print("Processing complete! Data saved in processed_data.json")