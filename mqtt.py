import signal
import sys
import ast
import ibmiotf.application
import ibmiotf.device


def MQTTconnect(deviceId):
    
    def interruptHandler(signal, frame):
        client.disconnect()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, interruptHandler)
    client = None
    
    A=False
    while not A:
        try:
            client = None
            r=open("/home/pi/_fieldtest/credentials.txt","r")
            cred=r.read()
            cred=ast.literal_eval(cred)
            r.close()
            print "cred file found"
            DeviceToken=cred["authToken"]
            DeviceId= cred["deviceId"]
            deviceOptions = {"org": "pddx55", "type":"externalDevice","id":DeviceId, "auth-method":"token","auth-token":DeviceToken}
            print deviceOptions
            client = ibmiotf.device.Client(deviceOptions)
            client.connect()
            A=True
        except:
            print "open file failed, cred not found or not authorized ---> creating new cred"
            apiOptions = {"org": "pddx55", "id":deviceId, "auth-method": "apikey", "auth-key": "a-pddx55-pcuh4ihl3o", "auth-token": "?ZY&QXOJ)CW2M5@ccK"} #fieldtest-dev
            client = ibmiotf.application.Client(apiOptions)
            client.connect()
            try:
                client.api.deleteDevice(typeId="externalDevice",deviceId=deviceId)
            except:
                print 'cannot delete device: ' + deviceId
        
            reg = client.api.registerDevice(typeId="externalDevice",deviceId=deviceId)
            if reg !="":
                r= open("/home/pi/_fieldtest/credentials.txt","w")
                r.write(str(reg))
                r.close()

            A=False
    
    return (client)


def MQTTget(myCommandCallback,client):
    try:
        client.commandCallback = myCommandCallback
    except ibmiotf.ConfigurationException as e:
        print(str(e))
        sys.exit()
    except ibmiotf.UnsupportedAuthenticationMethod as e:
        print(str(e))
        sys.exit()
    except ibmiotf.ConnectionException as e:
        print(str(e))
        sys.exit()
    
    print("(Press Ctrl+C to disconnect)")
    


