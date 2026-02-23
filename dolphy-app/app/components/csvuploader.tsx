"use client"

import { useState } from 'react';
import axios from 'axios';

const CsvUploader = ({ onCSVUpload }) => {
    const [err, setErr] = useState("");
    const [csvFile, setCsvFile] = useState(null);
    const [progress, updateProgress] = useState(0);

    const CHUNK_SIZE: int = 10 * 1024 * 1024;
    const handleFileUpload = (event) => {
        const file = event.target.files[0];
        if(!file) return;

        if(!file.name.endsWith(".csv")) {
            setErr("Add a CSV file.");
            return;
        }
        
        setCsvFile(file);
    };  

    const csvUpload = async () => {
        if (!csvFile) return alert("Please upload a csv file");

        let totalChunks = Math.ceil(csvFile.size / CHUNK_SIZE);
        const file_id = crypto.randomUUID();

        let start = 0;
        let chunk_idx = 0;

        while (start < csvFile.size){
            const end = Math.min(start + CHUNK_SIZE, csvFile.size);
            const chunk = csvFile.slice(start, end);
            
            const formData = new FormData();
            formData.append('csvChunk', chunk);
            formData.append('file_id', file_id);
            formData.append('chunk_index', chunk_idx);
            formData.append('total_chunks', totalChunks);
            try {
                const response = await axios.post('https://localhost:5001/api/data/upload', formData);
                chunk_idx++;

                start = end;
                updateProgress(Math.round((chunk_idx/totalChunks) * 100));
            } catch (err) {
                console.error("Batch upload failed", err);
                return;
            }
        }        
    }

    return (
        <div>
            <input type="file" accept=".csv" onChange={handleFileUpload} />
            <button onClick={csvUpload}> Upload CSV </button>
            {err && <p style={{ color: "red" }}>{err}</p>}
            {progress > 0 && <p>Progress: {progress}</p>}
        </div>
    );
};

export default CsvUploader; 
