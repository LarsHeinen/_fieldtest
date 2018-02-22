import signal
import sys
import ibmiotf.application


def MQTTconnect(deviceId):
    apiOptions = {"org": "pddx55", "id":deviceId, "auth-method": "apikey", "auth-key": "a-pddx55-pcuh4ihl3o", "auth-token": "?ZY&QXOJ)CW2M5@ccK"} #fieldtest-dev
    
    def interruptHandler(signal, frame):
        client.disconnect()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, interruptHandler)
    client = None
    client = ibmiotf.application.Client(apiOptions)
    client.connect()
    return (client)
    

def MQTTget(myEventCallback,myCommandCallback,client):
    try:
        client.deviceEventCallback = myEventCallback
        client.deviceCommandCallback = myCommandCallback
        client.subscribeToDeviceCommands(command='ssh')
        client.subscribeToDeviceCommands(command='configFile')
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
    
    print("=============================================================================")
    print("%-33s%-30s%s" % ("Timestamp", "Device", "Event"))
    print("=============================================================================")

def MQTTpostEvent(Topic,message,client,deviceType,deviceId):
    #Payload = {'d':{"sentAt":dt.datetime.utcnow().isoformat()[:-3]+'Z'}}
    #Payload['d'].update(message)
    Payload = {'d':message}
    print 'try to send event: ' + Topic
    client.publishEvent(deviceType, deviceId, Topic, "json", Payload)

def MQTTpostCommand(Topic,message,client,deviceType,deviceId):
    #Payload['c']["sentAt"] = dt.datetime.utcnow().isoformat()[:-3]+'Z'
    Payload = {'c':message}
    print 'try to send command: ' + Topic
    client.publishCommand(deviceType, deviceId, Topic, "json", Payload)

def messageBuilder(deviceId, fileName, cmpstr):
    message = {"a": {"id": deviceId, "filename":fileName, "content": cmpstr}}
    print 'message build'
    return message
    


