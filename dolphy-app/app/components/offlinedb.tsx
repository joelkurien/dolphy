"use client"

import { db } from "../db_components/csv_db.js"
import { useState, useEffect } from "react";
import Papa from "papaparse";
import { useLiveQuery } from "dexie-react-hooks"

const OfflineDB = ({ csvData }) => {
    const rows = useLiveQuery(() => db.table('csv_data')
                                      .toArray()
                                      .catch(() => []), []);  
    
    useEffect(() => {
        if(!csvData || !csvData.file) return;

        const { file } = csvData;

        Papa.parse(file, {
            header: true, 
            skipEmptyLines: true,
            complete: async (results) => {
                const data = results.data;
                if(data.length == 0) return; 
                
                const columns = Object.keys(data[0]).filter(c => c.trim() !== "");
                
                const schema = `++id, ${columns.join(', ')}`;

                
                db.close();
                db.version((db.verno || 0) + 1).stores({
                    csv_data: schema
                });

                try {
                    await db.open();
                    await db.csv_data.clear();
                    await db.csv_data.bulkAdd(data);
                    console.log(data.length);
                    console.log("New Table is cached");
                } catch(err) {
                    console.log("Schema failed to create a new table in the db: ", err);
                }
            }
        });
    }, [csvData]);
    

    if(!rows || rows.length === 0) return <p>No data in the CSV</p>;

    const headers = Object.keys(rows[0]).filter(key => key !== 'id');

    return (
        <table>
            <thead>
                <tr>
                    {headers.map( header => <th key={header}>{header}</th>)}
                </tr>
            </thead>
            <tbody>
                {rows.map((row) => (
                    <tr key={row.id}>
                        {headers.map(header => <td key={header}>{row[header]}</td>)}
                    </tr>
                ))}
            </tbody>
        </table>
    );
};

export default OfflineDB;
