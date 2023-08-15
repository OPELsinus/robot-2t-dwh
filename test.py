import os

a = []

for file in os.listdir(r'C:\Users\Abdykarim.D\PycharmProjects\robot-2t-dwh\working_path\1583_1'):
    a.append(file.replace('_1583', ''))

c = 0
c1 = 0
for file in os.listdir(r'C:\Users\Abdykarim.D\PycharmProjects\robot-2t-dwh\working_path\290'):
    if file in a:
        # print(i)
        c += 1
    else:
        print(file)
        c1 += 1

print(c, c1)
