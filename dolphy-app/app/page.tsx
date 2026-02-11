"use client"

import { useState, useEffect } from "react";
import Dexie from "dexie";
import FileUploader from "./components/fileuploader.tsx";
import OfflineDB from "./components/offlinedb.tsx";


export default function Home() {
  const [csvData, setCSVData] = useState(null);
  const [hasMounted, setHasMounted] = useState(false);

  useEffect(() => {
    setHasMounted(true);
  }, []);

  if(!hasMounted) return null;

  return (
      <div>
        <FileUploader onCSVUpload={setCSVData} />
        <OfflineDB csvData={csvData}/>
      </div>
  );
}
