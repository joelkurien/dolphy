"use client"

import Dexie from "dexie";

export const db = new Dexie('CSVFileDB');

db.version(1).stores({
    csv_data: '++id'
});
