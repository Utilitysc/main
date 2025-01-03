import asyncio
from pymodbus.client import AsyncModbusTcpClient
from pymodbus.exceptions import ModbusException
from tabulate import tabulate
import mysql.connector
import datetime

# Configuration
MODBUS_IP = "10.27.102.5"  # Shared IP address
MODBUS_PORT = 502           # Default Modbus TCP port
TOTAL_UNITS = 13            # Number of devices
READ_INTERVAL = 60          # Interval in seconds (1 minute)

# MySQL Database Configuration
MYSQL_CONFIG = {
    "host": "localhost",
    "user": "root",          # Replace with your MySQL username
    "password": "",          # Leave empty for no password
    "database": "vsd_monitoring",
}

# VSD Names Mapping
VSD_NAMES = {
    1: "CT_9", 2: "CT_10", 3: "CT_11", 4: "CT_12",
    5: "CT_13", 6: "CHWP_6", 7: "CHWP_7", 8: "CHWP_8",
    9: "CHWP_9", 10: "CCP_6", 11: "CCP_7", 12: "CCP_8", 13: "CCP_9"
}

def save_to_database(table_name, date, time, values):
    """
    Save a single row of readings to the specified MySQL database table.
    """
    try:
        # Connect to the MySQL database
        connection = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = connection.cursor()

        # Build placeholders dynamically
        placeholders = ", ".join(["%s"] * (2 + TOTAL_UNITS))
        insert_query = f"""
            INSERT INTO {table_name} (date, time, {', '.join(VSD_NAMES.values())})
            VALUES ({placeholders})
        """

        # Execute the query
        cursor.execute(insert_query, [date, time] + values)
        connection.commit()

    except mysql.connector.Error as err:
        print(f"Error saving to {table_name}: {err}")
    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()

async def read_vsd_data_batch(client, unit_id, start_register, count, scaling_factors, valid_ranges):
    """
    Reads multiple registers from a Modbus unit asynchronously and extracts specified data.
    """
    try:
        print(f"Reading unit {unit_id}, start_register {start_register}, count {count}")
        address = start_register - 40001  # Adjust for zero-based addressing
        response = await client.read_holding_registers(address, count=count, slave=unit_id)

        if response.isError():
            print(f"Error reading unit {unit_id}: {response}")
            return [None] * count

        values = []
        for i, raw_value in enumerate(response.registers):
            scaled_value = raw_value / scaling_factors[i]
            valid_range = valid_ranges[i]
            if valid_range[0] <= scaled_value <= valid_range[1]:
                values.append(scaled_value)
            else:
                print(f"Unit {unit_id}, Register {start_register + i}: Value {scaled_value} out of range {valid_range}")
                values.append(None)

        return values

    except Exception as e:
        print(f"Exception reading unit {unit_id}: {e}")
        return [None] * count

async def read_vsd_status(client, unit_id, start_register):
    """
    Reads a coil register for the status (RUN/STOP) using discrete inputs.
    """
    try:
        address = start_register - 10001  # Adjust for zero-based addressing
        response = await client.read_discrete_inputs(address=address, count=8, slave=unit_id)

        if not response.isError():
            bit_2 = response.bits[2]
            bit_3 = response.bits[3]
            bit_7 = response.bits[7]

            return ["RUN" if bit_2 else "STOP", "FAULT" if bit_3 else "NORMAL", "ALARM" if bit_7 else "NORMAL"]
        else:
            print(f"Error reading discrete inputs from unit {unit_id}: {response}")
            return [None, None, None]

    except ModbusException as e:
        print(f"ModbusException while reading status from unit {unit_id}: {e}")
        return [None, None, None]
    except Exception as e:
        print(f"Exception while reading status from unit {unit_id}: {e}")
        return [None, None, None]

async def main():
    # Create Modbus TCP client
    async with AsyncModbusTcpClient(MODBUS_IP, port=MODBUS_PORT) as client:

        while True:
            # Record current date and time
            now = datetime.datetime.now()
            date = now.strftime("%Y-%m-%d")
            time_str = now.strftime("%H:%M")

            # Initialize data lists
            temperatures, currents, frequencies = [], [], []
            run_status, fault_status, alarm_status = [], [], []

            for unit_id in range(1, TOTAL_UNITS + 1):
                # Read temperature, current, and frequency
                values = await read_vsd_data_batch(
                client,
                unit_id,
                40103,
                count=8,
                scaling_factors=[10, 10, 10, 10, 10, 10, 10, 10],  # Example scaling factors for each register
                 valid_ranges=[
                (0, 50),   # Frequency: 0-50 Hz
                (0, 200),  # Current: 0-200 A
                (-50, 100), # Temperature: -50°C to 100°C
                (0, 100),  # Add specific ranges for other registers as needed
                (0, 100), 
                (0, 100),
                (0, 100),
                (0, 100),  # Adjust or expand as needed for your registers
                ]
                )


                frequency, current, temperature = values[0], values[1], values[7]
                frequencies.append(frequency)
                currents.append(current)
                temperatures.append(temperature)

                # Read status
                status = await read_vsd_status(client, unit_id, start_register=10001)
                run_status.append(status[0])
                fault_status.append(status[1])
                alarm_status.append(status[2])

            # Save readings to database
            save_to_database("vsd_temp", date, time_str, temperatures)
            save_to_database("vsd_curr", date, time_str, currents)
            save_to_database("vsd_freq", date, time_str, frequencies)
            save_to_database("vsd_run", date, time_str, run_status)
           # save_to_database("vsd_fault", date, time_str, fault_status)
           # save_to_database("vsd_alarm", date, time_str, alarm_status)

            # Wait for the next cycle
            print("All units read. Waiting for the next interval...")
            await asyncio.sleep(READ_INTERVAL)

if __name__ == "__main__":
    asyncio.run(main())
