import os
from zipfile import ZipFile
 
import torch
from sentence_transformers import SentenceTransformer

#model = SentenceTransformer("models/dbpedia_custom_small")
model = SentenceTransformer("covid_tasb_9")
 
folder_path = "traced_model"
model_name = "trec_covid.pt"
zip_file_name = "trec_covid_tuned.zip"
 
save_json_folder_path = folder_path
model_output_path = folder_path
 
model_path = os.path.join(folder_path, model_name)
 
print("model_path:", model_path)
 
zip_file_path = os.path.join(model_output_path, zip_file_name)
 
# save tokenizer.json in save_json_folder_name
model.save(save_json_folder_path)
 
# convert to pt format will need to be in cpu,
# set the device to cpu, convert its input_ids and attention_mask in cpu and save as .pt format
device = torch.device("cpu")
cpu_model = model.to(device)
 
sentences = ["This is the first example we want to explore", "I'm using these sentences as example but please try to provide longer example which will be helpful for models"]
 
features = cpu_model.tokenizer(
    sentences, return_tensors="pt", padding=True, truncation=True
).to(device)
 
compiled_model = torch.jit.trace(
    cpu_model,
    (
        {
            "input_ids": features["input_ids"],
            "attention_mask": features["attention_mask"],
        }
    ),
    strict=False,
)
torch.jit.save(compiled_model, model_path)
print("model file is saved to ", model_path)
 
# zip model file along with tokenizer.json as output
with ZipFile(str(zip_file_path), "w") as zipObj:
    zipObj.write(
        model_path,
        arcname=str(model_name),
    )
    zipObj.write(
        os.path.join(save_json_folder_path, "tokenizer.json"),
        arcname="tokenizer.json",
    )
print("zip file is saved to ", zip_file_path, "\n")
