"use client"

import { useState, useEffect } from "react";
import Dexie from "dexie";
import CsvUploader from "./components/csvuploader";
import OfflineDB from "./components/offlinedb";

export default function Home() {
  const [csvData, setCSVData] = useState(null);
  const [hasMounted, setHasMounted] = useState(false);

  useEffect(() => {
    setHasMounted(true);
  }, []);

  if(!hasMounted) return null;

  return (
      <div>
        <CsvUploader onCSVUpload={setCSVData} />
        <OfflineDB csvData={csvData}/>
      </div>
  );
}
