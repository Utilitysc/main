import asyncio
from pymodbus.client import AsyncModbusTcpClient
from pymodbus.exceptions import ModbusException
from tabulate import tabulate
import sqlite3
import datetime
import os

# Configuration
MODBUS_IP = "10.27.102.5"  # Shared IP address
MODBUS_PORT = 502           # Default Modbus TCP port
TOTAL_UNITS = 13            # Number of devices
READ_INTERVAL = 60          # Interval in seconds (1 minute)

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
    Initialize the SQLite database with separate tables for temperature, current, frequency, and status readings.
    """
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()

    # Create temperature table
    temp_columns = ", ".join([f"{name} REAL" for name in VSD_NAMES.values()])
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS vsd_temp (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            time TEXT NOT NULL,
            {temp_columns}
        )
    """)

    # Create current table
    current_columns = ", ".join([f"{name} REAL" for name in VSD_NAMES.values()])
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS vsd_current (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            time TEXT NOT NULL,
            {current_columns}
        )
    """)

    # Create frequency table
    freq_columns = ", ".join([f"{name} REAL" for name in VSD_NAMES.values()])
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS vsd_freq (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            time TEXT NOT NULL,
            {freq_columns}
        )
    """)

    # Create status tables
    for table in ["vsd_run", "vsd_fault", "vsd_alarm"]:
        status_columns = ", ".join([f"{name} TEXT" for name in VSD_NAMES.values()])
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                time TEXT NOT NULL,
                {status_columns}
            )
        """)

    conn.commit()
    conn.close()

def save_to_database(table_name, date, time, values):
    """
    Save a single row of readings to the specified SQLite database table.
    """
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()

    # Build placeholders dynamically based on TOTAL_UNITS
    placeholders = ", ".join(["?"] * (2 + TOTAL_UNITS))  # 2 for date and time, the rest for values

    # Insert query
    insert_query = f"""
        INSERT INTO {table_name} (date, time, {', '.join(VSD_NAMES.values())})
        VALUES ({placeholders})
    """
    cursor.execute(insert_query, [date, time] + values)
    conn.commit()
    conn.close()

async def read_vsd_data_batch(client, unit_id, start_register, count, scaling_factors, valid_ranges):
    """
    Reads multiple registers from a Modbus unit asynchronously and extracts specified data.

    Parameters:
    - client: AsyncModbusTcpClient instance.
    - unit_id: ID of the Modbus unit to read.
    - start_register: Starting register address.
    - count: Number of registers to read.
    - scaling_factors: List of scaling factors for each register value.
    - valid_ranges: List of valid ranges (min, max) for each register value.

    Returns:
    - List of processed values (None if invalid or failed).
    """
    try:
        address = start_register - 40001  # Adjust for zero-based addressing
        response = await client.read_holding_registers(address, count=count, slave=unit_id)

        if response.isError():
            print(f"{VSD_NAMES[unit_id]}: Modbus Error")
            return [None] * count

        values = []
        for i, raw_value in enumerate(response.registers):
            scaled_value = raw_value / scaling_factors[i]
            valid_range = valid_ranges[i]

            if valid_range[0] <= scaled_value <= valid_range[1]:
                values.append(scaled_value)
            else:
                print(f"{VSD_NAMES[unit_id]}: Value {scaled_value} out of range {valid_range}")
                values.append(None)

        return values

    except Exception as e:
        print(f"{VSD_NAMES[unit_id]}: Exception - {e}")
        return [None] * count

async def read_vsd_status(client, unit_id, start_register):
    """
    Reads a coil register for the status (RUN/STOP) using discrete inputs.
    """
    try:
        address = start_register - 10001  # Adjust for zero-based addressing
        response = await client.read_discrete_inputs(address=address, count=8, slave=unit_id)

        if not response.isError():
            # Extract bit 2, 3, and 7 (0-based indexing)
            bit_2 = response.bits[2]
            bit_3 = response.bits[3]
            bit_7 = response.bits[7]

            # Return status as 'RUN' or 'STOP'
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
    # Initialize the database
    initialize_database()

    # Create Modbus TCP client
    async with AsyncModbusTcpClient(MODBUS_IP, port=MODBUS_PORT) as client:

        # Prepare table headers for display
        temp_headers = ["Date", "Time"] + list(VSD_NAMES.values())
        current_headers = ["Date", "Time"] + [f"Current_{name}" for name in VSD_NAMES.values()]
        freq_headers = ["Date", "Time"] + [f"Freq_{name}" for name in VSD_NAMES.values()]
        status_headers = ["Date", "Time"] + [f"Status_{name}" for name in VSD_NAMES.values()]

        while True:
            # Record current date and time
            now = datetime.datetime.now()
            date = now.strftime("%Y-%m-%d")
            time_str = now.strftime("%H:%M")

            # Read all devices
            temperatures = []
            currents = []
            frequencies = []
            run_status = []
            fault_status = []
            alarm_status = []

            for unit_id in range(1, TOTAL_UNITS + 1):
                # Read temperature, current, and frequency
                values = await read_vsd_data_batch(
                    client, unit_id, 40103, count=8,
                    scaling_factors=[10, 10, 10, 10, 10, 10, 10, 10],
                    valid_ranges=[(0, 50), (0, 200), (-50, 100)] + [(-50, 100)] * 5
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
            save_to_database("vsd_current", date, time_str, currents)
            save_to_database("vsd_freq", date, time_str, frequencies)
            save_to_database("vsd_run", date, time_str, run_status)
            save_to_database("vsd_fault", date, time_str, fault_status)
            save_to_database("vsd_alarm", date, time_str, alarm_status)

            # Display readings
            temp_row = [date, time_str] + temperatures
            current_row = [date, time_str] + currents
            freq_row = [date, time_str] + frequencies
            run_row = [date, time_str] + run_status
            fault_row = [date, time_str] + fault_status
            alarm_row = [date, time_str] + alarm_status

            print("\nTemperature Readings:")
            print(tabulate([temp_row], headers=temp_headers, tablefmt="grid"))

            print("\nCurrent Readings:")
            print(tabulate([current_row], headers=current_headers, tablefmt="grid"))

            print("\nFrequency Readings:")
            print(tabulate([freq_row], headers=freq_headers, tablefmt="grid"))

            print("\nRUN Status:")
            print(tabulate([run_row], headers=status_headers, tablefmt="grid"))

            print("\nFAULT Status:")
            print(tabulate([fault_row], headers=status_headers, tablefmt="grid"))

            print("\nALARM Status:")
            print(tabulate([alarm_row], headers=status_headers, tablefmt="grid"))

            # Wait for the next cycle
            print("All units read. Waiting for the next interval...")
            await asyncio.sleep(READ_INTERVAL)

if __name__ == "__main__":
    asyncio.run(main())
