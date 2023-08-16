import os
import shutil






















































a = []

for file in os.listdir(r'C:\Users\Abdykarim.D\PycharmProjects\robot-2t-dwh\working_path\1583'):
    a.append(file.split('_')[-1].split('.')[0])

print(a)
c = 0
c1 = 0
for file in os.listdir(r'C:\Users\Abdykarim.D\PycharmProjects\robot-2t-dwh\working_path\290'):
    if file.split('_')[-1].split('.')[0] in a:

        c += 1
    else:
        print(file)
        c1 += 1
print(c)
print(c1)

