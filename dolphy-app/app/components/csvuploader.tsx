"use client"

import { useState } from 'react';
import Papa from "papaparse";

const CsvUploader = ({ onCSVUpload }) => {
    const [err, setErr] = useState("");

    const handleFileUpload = (event) => {
        const file = event.target.files[0];
        if(!file) return;

        if(!file.name.endsWith(".csv")) {
            setErr("Add a CSV file.");
            return;
        }

        const reader = new FileReader();
        reader.onload = (e) => {
            const text = e.target.result;
            Papa.parse(text, {
                header: true,
                skipEmptyLines: true,
                complete: (results) => {
                    onCSVUpload({
                        file: file
                    });
                },
                error: (err) => {
                    setErr("Error parsing CSV: " + err.message);
                },
            });
        };
        reader.readAsText(file);
    };  

    return (
        <div>
            <input type="file" accept=".csv" onChange={handleFileUpload} />
            {err && <p style={{ color: "red" }}>{err}</p>}
            
        </div>
    );
};

export default CsvUploader; 
