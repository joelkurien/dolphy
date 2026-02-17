"use client"

import { useState } from 'react';
import { db } from "../db_components/csv_db"

const CleanerForm = ({ headers }) => {
    const [err, setErr] = useState("");

    const baseFormData = headers.reduce((acc, header) => {
        acc[header] = {'datatype': 0, 'clean-method': [] as string[]};
        return acc;
    }, {} as Record<string, {'datatype': number, 'clean-method': string[]}>) 
   
    const [formData, setFormData] = useState(baseFormData);

    const typeOptions = [1,2,3,4,5];

    const cleanOptions = {
        1: ['string-int', 'remove null', 'mode impute',
            'seq-gap-analysis', 'ranking', 
            'create indicate outlier column', 'remove outlier'],
        2: ['rounding', 'mean-impute', 'median-impute', 'linear-interpolate',
                         'epsilon-comparison', 'min-max scaling', 'z-score standardize',
                         'winsorize','log-transform', 'Box-cox transform', 'unit norm', 
                         'moving average' , 'sigmoid squash', 'tanh squash', 
                         'knn-impute', 'mice', 'indicator-flag', 
                         'outlier-detect'],
        3: ['case-normalize', 'whitespace-trim', 'punctuation-removal', 
                         'special-character-strip', 'stemming', 'HTML strip', 'XML strip',
                         'fuzzy-matching', 'concatentation', 'splitting-spaces'],
        4: ['ISO-standardization', 'timezone-localization', 'unix-epoch-conv', 
            'remove-future-dates', 'fill-imputation', 'interpolation'],
        5: ['synonym-matching', 'null-to-false', 'inversion', 'majority-vote',
                         'one-hot-encoding', 'set-to-binary']
    };

    const handleTypeChange = (header: string, value: number) => {
        setFormData(prev => ({
            ...prev, 
            [header]: {'datatype': value, 'clean-method': []}
        }));
    };

    const handleAddCleanMethod = (header: string, filter: string) => {
        setFormData(prev => ({
            ...prev,
            [header]: {
                ...prev[header],
                'clean-method': [...prev[header]['clean-method'], filter]
            }
        }));
    };

    const handleRemoveFilter = (header: string, index: number) => {
        setFormData(prev => ({
            ...prev,
            [header]: {
                ...prev[header],
                'clean-method': prev[header]['clean-method'].filter((_, idx) => idx !== index)
            }
        }));
    };

    const handleMoveCleanMethod = (header: string, fromIndex: number, direction: 'up' | 'down') => {
        const methods = [...formData[header]['clean-method']];
        const toIndex = direction === 'up' ? fromIndex - 1 : fromIndex + 1;

        if (toIndex < 0 || toIndex >= methods.length) return;

        // Swap
        [methods[fromIndex], methods[toIndex]] = [methods[toIndex], methods[fromIndex]];

        setFormData(prev => ({
            ...prev,
            [header]: {
                ...prev[header],
                'clean-method': methods
            }
        }));
    };
    
    const handleSubmit = (e: React.FormEvent) => {
        e.preventDefault();
        console.log("Submitted data: ", formData);
    };

    console.log(formData);
    return (
        <form onSubmit={handleSubmit}>
            {Object.entries(formData).map(([header, fields]) => (
                <fieldset key={header} style={{ 
                    marginBottom: '20px', 
                    padding: '15px', 
                    border: '1px solid #ccc',
                    borderRadius: '5px'
                }}>
                    <legend><strong>{header}</strong></legend>
                    
                    {/* Datatype Selection */}
                    <div style={{ marginBottom: '15px' }}>
                        <label>
                            Data Type:
                            <select
                                value={fields.datatype}
                                onChange={(e) => handleTypeChange(header, Number(e.target.value))}
                                style={{ marginLeft: '10px', padding: '5px', minWidth: '150px' }}
                            >
                                <option value={0}>-- Select Type --</option>
                                {typeOptions.map(option => (
                                    <option key={option} value={option}>
                                        {option}
                                    </option>
                                ))}
                            </select>
                        </label>
                    </div>

                    {/* Clean Method Selection */}
                    {fields.datatype > 0 && (
                        <div>
                            <label style={{ display: 'block', marginBottom: '8px' }}>
                                Add Clean Methods:
                            </label>
                            <select
                                onChange={(e) => {
                                    if (e.target.value && !fields['clean-method'].includes(e.target.value)) {
                                        handleAddCleanMethod(header, e.target.value);
                                        e.target.value = ''; // Reset dropdown
                                    }
                                }}
                                style={{ padding: '5px', minWidth: '200px' }}
                                defaultValue=""
                            >
                                <option value="">-- Add a clean method --</option>
                                {cleanOptions[fields.datatype]?.map(option => (
                                    <option 
                                        key={option} 
                                        value={option}
                                        disabled={fields['clean-method'].includes(option)}
                                    >
                                        {option}
                                    </option>
                                ))}
                            </select>

                            {/* Selected Clean Methods with Reordering */}
                            {fields['clean-method'].length > 0 && (
                                <div style={{ 
                                    marginTop: '15px', 
                                    padding: '10px', 
                                    backgroundColor: '#f8f9fa',
                                    borderRadius: '4px'
                                }}>
                                    <strong style={{ display: 'block', marginBottom: '10px' }}>
                                        Clean Method Pipeline (applied in order):
                                    </strong>
                                    {fields['clean-method'].map((method, index) => (
                                        <div 
                                            key={index} 
                                            style={{ 
                                                display: 'flex', 
                                                alignItems: 'center', 
                                                gap: '10px',
                                                padding: '8px',
                                                backgroundColor: 'white',
                                                marginBottom: '5px',
                                                borderRadius: '4px',
                                                border: '1px solid #dee2e6'
                                            }}
                                        >
                                            <span style={{ flex: 1 }}>
                                                {method}
                                            </span>

                                            {/* Move Up Button */}
                                            <button
                                                type="button"
                                                onClick={() => handleMoveCleanMethod(header, index, 'up')}
                                                disabled={index === 0}
                                                style={{ 
                                                    padding: '4px 8px',
                                                    cursor: index === 0 ? 'not-allowed' : 'pointer',
                                                    opacity: index === 0 ? 0.5 : 1
                                                }}
                                                title="Move up"
                                            >
                                                ↑
                                            </button>

                                            {/* Move Down Button */}
                                            <button
                                                type="button"
                                                onClick={() => handleMoveCleanMethod(header, index, 'down')}
                                                disabled={index === fields['clean-method'].length - 1}
                                                style={{ 
                                                    padding: '4px 8px',
                                                    cursor: index === fields['clean-method'].length - 1 ? 'not-allowed' : 'pointer',
                                                    opacity: index === fields['clean-method'].length - 1 ? 0.5 : 1
                                                }}
                                                title="Move down"
                                            >
                                                ↓
                                            </button>

                                            {/* Remove Button */}
                                            <button
                                                type="button"
                                                onClick={() => handleRemoveCleanMethod(header, index)}
                                                style={{ 
                                                    padding: '4px 8px',
                                                    backgroundColor: '#dc3545',
                                                    color: 'white',
                                                    border: 'none',
                                                    borderRadius: '3px',
                                                    cursor: 'pointer'
                                                }}
                                                title="Remove method"
                                            >
                                                ✕
                                            </button>
                                        </div>
                                    ))}
                                </div>
                            )}
                        </div>
                    )}

                </fieldset>
            ))}
            
            <button type="submit" style={{ 
                padding: '10px 20px', 
                marginTop: '10px',
                backgroundColor: '#007bff',
                color: 'white',
                border: 'none',
                borderRadius: '4px',
                cursor: 'pointer',
                fontSize: '16px'
            }}>
                Submit
            </button>
        </form>
    );
};
export default CleanerForm; 
