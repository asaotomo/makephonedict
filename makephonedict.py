import json
import requests
import base64
import csv


def make_dict(total, city, isp):
    url = base64.b64decode("aHR0cDovL2FwcC53ZXdlNzc4OC5jbjo4MC9hcHAvUXVlcnlTaHVmZmxlQ29kZT90b2tlbj0=")
    headers = json.loads(base64.b64decode(
        "eyJBY2NlcHQtRW5jb2RpbmciOiAiZGVmbGF0ZSwgZ3ppcCIsICJPcmlnaW4iOiAiaHR0cDovL2FwcC53ZXdlNzc4OC5jbiIsICJYLVJlcXVlc3RlZC1XaXRoIjogIlhNTEh0dHBSZXF1ZXN0IiwgIlVzZXItQWdlbnQiOiAiTW96aWxsYS81LjAgKFdpbmRvd3M7IFU7IFdpbmRvd3MgTlQgNi4yOyB6aC1DTikgQXBwbGVXZWJLaXQvNTMzKyAoS0hUTUwsIGxpa2UgR2Vja28pIiwgIkNvbnRlbnQtVHlwZSI6ICJhcHBsaWNhdGlvbi94LXd3dy1mb3JtLXVybGVuY29kZWQ7IGNoYXJzZXQ9VVRGLTgiLCAiQWNjZXB0IjogImFwcGxpY2F0aW9uL2pzb24sIHRleHQvamF2YXNjcmlwdCwgKi8qOyBxPTAuMDEiLCAiUmVmZXJlciI6ICJodHRwOi8vYXBwLndld2U3Nzg4LmNuL2FwcC9mbGFzaCJ9").decode(
        'utf-8'))
    data = {"total": total, "city": city, "isp": isp}
    data = requests.post(url, headers=headers, data=data).json()
    prefix = data['prefix']
    suffix = data['suffix']
    prefixInfo = data['prefixInfo']
    no = 0

    with open("telephone_number_dict.csv", "w+", encoding="UTF-8", newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["手机号", "手机号归属地"])
        for i in range(len(prefix)):
            for n in range(len(suffix)):
                phoneNum = prefix[i] + suffix[n]
                phoneInfo = prefixInfo[i]['province'] + prefixInfo[i]['city'] + prefixInfo[i]['isp']
                print(phoneNum, phoneInfo)
                writer.writerow([phoneNum, phoneInfo])
                no += 1
    print("[*]手机号字典（telephone_number_dict.csv）生成成功，共计生成：{}条，请打开字典查看详细内容！".format(no))


if __name__ == '__main__':
    try:
        print(""" __  __       _        ____  _                      ____  _      _   
|  \/  | __ _| | _____|  _ \| |__   ___  _ __   ___|  _ \(_) ___| |_ 
| |\/| |/ _` | |/ / _ \ |_) | '_ \ / _ \| '_ \ / _ \ | | | |/ __| __|
| |  | | (_| |   <  __/  __/| | | | (_) | | | |  __/ |_| | | (__| |_ 
|_|  |_|\__,_|_|\_\___|_|   |_| |_|\___/|_| |_|\___|____/|_|\___|\__|
Hx0战队-手机号字典生成器V1.1               Update:2025.02.26                                        
        """)
        total = input("[+]请输入要生成的手机号数量(>=1024):")
        city = input(
            "[+]请输入要生成的手机号码城市区域代码(如北京，请输入1101,其他城市代码见城市区域代码查询表，同时生成多城市请用英文逗号[,]分割):")
        isp = input(
            "[+]请输入要生成的手机号运营商代码(4001-移动，4006-联通，4008-电信)(同时生成多个运营商请用英文逗号[,]分割，全部运营商请输入allin):")
        if isp == "allin":
            isp = "4001,4006,4008"
        make_dict(total, city.replace("，",","), isp.replace("，",","))
    except Exception as e:
        print(e)
