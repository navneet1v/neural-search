import hashlib

#Example: model_file_path = "/home/ec2-user/dev/norm_comb_tuned/test/trace_models/dbpedia_tuned.zip"
model_file_path = "<FULL_PATH_OF_THE_TRACED_MODEL>"

sha256 = hashlib.sha256()
BUF_SIZE = 65536  # lets read stuff in 64kb chunks!
with open(model_file_path, "rb") as file:
    while True:
        chunk = file.read(BUF_SIZE)
        if not chunk:
            break
        sha256.update(chunk)
sha256_value = sha256.hexdigest()

print(sha256_value)
