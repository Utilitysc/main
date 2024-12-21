import asyncio
from pymodbus.client import AsyncModbusTcpClient
from pymodbus.exceptions import ModbusException
from tabulate import tabulate
import sqlite3
import datetime
import os

# Configuration
MODBUS_IP = "10.27.102.5"
MODBUS_PORT = 502
TOTAL_UNITS = 13
READ_INTERVAL = 60  # seconds

# VSD Names Mapping
VSD_NAMES = {
    1: "CT_9", 2: "CT_10", 3: "CT_11", 4: "CT_12",
    5: "CT_13", 6: "CHWP_6", 7: "CHWP_7", 8: "CHWP_8",
    9: "CHWP_9", 10: "CCP_6", 11: "CCP_7", 12: "CCP_8", 13: "CCP_9"
}

# Database file path
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATABASE_NAME = os.path.join(SCRIPT_DIR, "vsd_read.db")

def initialize_database():
    """
    Initialize the SQLite database with separate tables for each reading type.
    """
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()

    # Create tables
    for table_name in ["vsd_temp", "vsd_current", "vsd_freq", "vsd_run_stop", "vsd_fault_normal", "vsd_alarm_normal"]:
        columns = ", ".join([f"{name} TEXT" for name in VSD_NAMES.values()])
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                time TEXT NOT NULL,
                {columns}
            )
        """)

    conn.commit()
    conn.close()

def save_to_database(table_name, date, time, values):
    """
    Save readings to the specified SQLite database table.
    """
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()

    placeholders = ", ".join(["?"] * (2 + TOTAL_UNITS))
    insert_query = f"""
        INSERT INTO {table_name} (date, time, {', '.join(VSD_NAMES.values())})
        VALUES ({placeholders})
    """
    cursor.execute(insert_query, [date, time] + values)
    conn.commit()
    conn.close()

async def read_vsd_data_batch(client, unit_id, start_register, count, scaling_factors, valid_ranges):
    """
    Reads multiple registers from a Modbus unit asynchronously and extracts data.
    """
    try:
        address = start_register - 40001
        response = await client.read_holding_registers(address, count=count, slave=unit_id)

        if response.isError():
            print(f"{VSD_NAMES[unit_id]}: Modbus Error")
            return [None] * count

        values = []
        for i, raw_value in enumerate(response.registers):
            scaled_value = raw_value / scaling_factors[i]
            valid_range = valid_ranges[i]
            values.append(scaled_value if valid_range[0] <= scaled_value <= valid_range[1] else None)

        return values
    except Exception as e:
        print(f"{VSD_NAMES[unit_id]}: Exception - {e}")
        return [None] * count

async def read_vsd_status(client, unit_id):
    """
    Reads a coil register for specific bits (2, 3, and 7).
    """
    try:
        address = 10004 - 10001
        response = await client.read_discrete_inputs(address=address, count=8, slave=unit_id)

        if not response.isError():
            return [
                "RUN" if response.bits[2] else "STOP",
                "FAULT" if response.bits[3] else "NORMAL",
                "ALARM" if response.bits[7] else "NORMAL"
            ]
        else:
            print(f"Error reading status from unit {unit_id}: {response}")
            return [None, None, None]
    except Exception as e:
        print(f"Exception reading status from unit {unit_id}: {e}")
        return [None, None, None]

async def main():
    initialize_database()

    async with AsyncModbusTcpClient(MODBUS_IP, port=MODBUS_PORT) as client:
        while True:
            try:
                # Fetch current date and time
                now = datetime.datetime.now()
                date = now.strftime("%Y-%m-%d")
                time_str = now.strftime("%H:%M")

                # Containers for data
                run_statuses = []
                fault_statuses = []
                alarm_statuses = []
                temperature_readings = []
                current_readings = []
                frequency_readings = []

                # Iterate over each VSD unit
                for unit_id in range(1, TOTAL_UNITS + 1):
                    # Read discrete inputs
                    status = await read_vsd_status(client, unit_id)
                    if status:
                        run_statuses.append(status[0])   # Run/Stop status
                        fault_statuses.append(status[1]) # Fault status
                        alarm_statuses.append(status[2]) # Alarm status
                    else:
                        run_statuses.append("Error")
                        fault_statuses.append("Error")
                        alarm_statuses.append("Error")

                    # Read holding registers for temperature, current, and frequency
                    temperature = await read_vsd_data_batch(
                        client, unit_id, start_register=40103, count=1,
                        scaling_factors=[100], valid_ranges=[(-50, 100)]
                    )
                    current = await read_vsd_data_batch(
                        client, unit_id, start_register=40104, count=1,
                        scaling_factors=[100], valid_ranges=[(0, 200)]
                    )
                    frequency = await read_vsd_data_batch(
                        client, unit_id, start_register=40105, count=1,
                        scaling_factors=[100], valid_ranges=[(0, 50)]
                    )

                    temperature_readings.append(temperature[0] if temperature[0] is not None else "Error")
                    current_readings.append(current[0] if current[0] is not None else "Error")
                    frequency_readings.append(frequency[0] if frequency[0] is not None else "Error")

                # Save to respective tables
                save_to_database("vsd_run", date, time_str, run_statuses)
                save_to_database("vsd_fault", date, time_str, fault_statuses)
                save_to_database("vsd_alarm", date, time_str, alarm_statuses)
                save_to_database("vsd_temp", date, time_str, temperature_readings)
                save_to_database("vsd_current", date, time_str, current_readings)
                save_to_database("vsd_frequency", date, time_str, frequency_readings)

                # Log data to the console
                print("\nRun/Stop Status:")
                print(tabulate([[date, time_str] + run_statuses], headers=["Date", "Time"] + list(VSD_NAMES.values()), tablefmt="grid"))
                print("\nFault Status:")
                print(tabulate([[date, time_str] + fault_statuses], headers=["Date", "Time"] + list(VSD_NAMES.values()), tablefmt="grid"))
                print("\nAlarm Status:")
                print(tabulate([[date, time_str] + alarm_statuses], headers=["Date", "Time"] + list(VSD_NAMES.values()), tablefmt="grid"))
                print("\nTemperature Readings:")
                print(tabulate([[date, time_str] + temperature_readings], headers=["Date", "Time"] + list(VSD_NAMES.values()), tablefmt="grid"))
                print("\nCurrent Readings:")
                print(tabulate([[date, time_str] + current_readings], headers=["Date", "Time"] + list(VSD_NAMES.values()), tablefmt="grid"))
                print("\nFrequency Readings:")
                print(tabulate([[date, time_str] + frequency_readings], headers=["Date", "Time"] + list(VSD_NAMES.values()), tablefmt="grid"))

                # Wait before next read
                await asyncio.sleep(READ_INTERVAL)

            except Exception as e:
                print(f"Error: {e}")
                
if __name__ == "__main__":
    asyncio.run(main())