import os


allErrFiles = []
for file in os.listdir("."):
    if file.startswith("run") and file.endswith(".log"):
        lines = open(file,"r").readlines()

        flag = False
        for line in lines:
            if "FAIL" in line:
                allErrFiles.append(lines[0].strip().split(":")[1].strip())
                flag = True
                break
        if not flag:
            os.remove(file)


print(allErrFiles)

for file in os.listdir("."):
    if file.startswith("log_raft") and file.endswith(".log"):
        if file not in allErrFiles:

            os.remove(file)

