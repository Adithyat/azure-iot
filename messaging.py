# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

import os
import asyncio
import uuid
from azure.iot.device.aio import IoTHubDeviceClient
from azure.iot.device import Message
import time

messages_to_send = 10


async def main():
    # The client object is used to interact with your Azure IoT hub.
    device_client = IoTHubDeviceClient.create_from_connection_string("HostName=RBPI-demo.azure-devices.net;DeviceId=temp_demo;SharedAccessKey=5XLNTIOlGmpyvNBcT3w0IbGXAoh5NjjCSRptPGBCUFc=")

    # Connect the client.
    await device_client.connect()

    async def send_test_message(i):
        time.sleep(1)
        print("sending message #" + str(i))
        msg = Message("{ \"MGPID\": 48, \"MGPLabel\": \"X_tilt\", \"MGPName\": \"X_tilt\", \"Timestamp\": \"2020-08-12 17:57:02\", \"usec\": 492949, \"ParamVal\": \"88.000000\" }")
        msg.message_id = uuid.uuid4()
        msg.correlation_id = "correlation-1234"
        msg.custom_properties = {
                                "CustomerId": "1",
                                "ProgramId": 2,
                                "RegionId": 1964,
                                "DeviceType": "4a",
                                "MessageVersion": 23,
                                "VIN":3215,
                                "ESN":4232545,
                                "DeviceSignalTimestamp": 7,
                                "Provider": "Dana",
                                "DeviceReleaseVersion": 1.0,
                                "PlantId": 38
                                }
        await device_client.send_message(msg)
        print("done sending message #" + str(i))
        

    # send `messages_to_send` messages in parallel
    await asyncio.gather(*[send_test_message(i) for i in range(1, messages_to_send + 1)])

    # finally, disconnect
    await device_client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())

    # If using Python 3.6 or below, use the following code instead of asyncio.run(main()):
    # loop = asyncio.get_event_loop()
    # loop.run_until_complete(main())
    # loop.close()