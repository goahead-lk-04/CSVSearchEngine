//
//  IndexManager.swift
//  CVSSearchEngineApp
//
//  Created by Lisa Kuchyna on 2024-11-14.
//

import Foundation


/// IndexManager class responsible for managing an index of CSV data for efficient searching and manipulation.
/// It supports caching rows, adding index entries, filtering, sorting, and finding duplicates.
/// Also manages external index for saving row number and its position in CSV file
public class IndexManager {
    /// Maps field name -> value -> row numbers
    public var primaryIndex: [String: [String: [Int]]] = [:]
    
    private var rowData: [Int: CSVRow] = [:]
    
    private var cachedRows: [Int: CSVRow] = [:]
    
    /// Maps row number -> UInt64
    private var rowPositions: [Int: UInt64] = [:]

    func addRowPosition(rowID: Int, position: UInt64) {
        rowPositions[rowID] = position
    }

    func getRowPosition(_ rowID: Int) -> UInt64? {
        return rowPositions[rowID]
    }

    func getCachedRow(_ rowID: Int) -> CSVRow? {
        return cachedRows[rowID]
    }

    func cacheRow(_ rowID: Int, row: CSVRow) {
        cachedRows[rowID] = row
    }

    public func addToIndex(field: String, value: String, rowNumber: Int, row: CSVRow) {
        let field_adj = field.lowercased()
        var value_adj = value.lowercased()
        if value_adj.isEmpty {
            value_adj = "null"
        }
        if primaryIndex[field_adj] == nil {
            primaryIndex[field_adj] = [:]
        }
        if primaryIndex[field_adj]?[value_adj] == nil {
            primaryIndex[field_adj]?[value_adj] = []
        }
        primaryIndex[field_adj]?[value_adj]?.append(rowNumber)
        
        rowData[rowNumber] = row
    }
        
    public func getRowPositions(field: String, value: String) -> [Int]? {
        return primaryIndex[field]?[value]
    }
    
    /// Prints a debug representation of the entire index, showing each field and its associated values and rows.
    public func debugPrintIndex() {
        for (field, index) in primaryIndex {
            print("Field: \(field)")
            for (value, rows) in index {
                print("  Value: \(value) -> Rows: \(rows)")
            }
        }
    }
    
    /** Finds and returns a dictionary of duplicate values for a specific field.
    
        - Parameter field: the field name to check for duplicates.
        - Returns: a dictionary mapping duplicate values to their associated row numbers.
    **/
    public func findDuplicates(forField field: String) -> [String: [Int]] {
        var duplicates: [String: [Int]] = [:]
       
        if let fieldIndex = primaryIndex[field] {
            print(fieldIndex)
            for (value, rows) in fieldIndex {
                if rows.count > 1 {
                   
                    duplicates[value] = rows
                }
            }
        }
        
        return duplicates
    }
    
    /** Retrieves the row numbers that have a missing value (`null`) for a specific field.
        
        - Parameter field: the field name to check for missing values.
        - Returns: an array of row numbers that have missing values for the specified field.
    **/
    public func getRowsWithMissingValue(forField field: String) -> [Int] {
        guard let fieldIndex = primaryIndex[field] else {
            return []
        }
        var rowsWithMissingValues: [Int] = []
        for (value, rows) in fieldIndex {
            if value.elementsEqual("null") {
                rowsWithMissingValues.append(contentsOf: rows)
            }
        }
        return rowsWithMissingValues
    }

    /** Retrieves the unique values for a specific field.
    
        - Parameter field: the field name to fetch unique values for.
        - Returns: a set of unique values for the specified field, or `nil` if the field doesn't exist in the index.
    **/
    public func getUniqueValues(forField field: String) -> Set<String>? {
        guard let fieldIndex = primaryIndex[field] else {
            return nil
        }
        return Set(fieldIndex.keys)
    }

    // MARK: - Row Sorting and Filtering
        
    /** Counts the number of occurrences of a specific value under a given field.
        
        - Parameters:
            - field: the field name.
            - value: the value to count occurrences of.
        - Returns: the count of rows that contain the specified value under the field.
    **/
    public func countOccurrences(forField field: String, value: String) -> Int {
        guard let fieldIndex = primaryIndex[field], let rows = fieldIndex[value] else {
            return 0
        }
        return rows.count
    }

    /** Sorts the row numbers associated with a specific field in ascending or descending order.
        
        - Parameters:
            - field: the field name to sort by.
            - ascending: a Boolean indicating whether the sort should be ascending (true) or descending (false).
        - Returns: a sorted array of row numbers for the specified field.
    **/
    public func sortRows(byField field: String, ascending: Bool = true) -> [Int]? {
        guard let fieldIndex = primaryIndex[field] else {
            return nil
        }
        var sortedRows = fieldIndex.flatMap { $0.value }
        if ascending {
            sortedRows.sort { $0 < $1 }
        } else {
            sortedRows.sort { $0 > $1 }
        }
        return sortedRows
    }

    /** Filters rows by an exact value under a specific field.
            
        - Parameters:
     
            - field: the field name to filter by.
            - value: the value to filter rows by.
        - Returns: an array of row numbers that contain the exact value under the given field.
    **/
    public func filterRowsByExactValue(forField field: String, value: String) -> [Int]? {
        guard let fieldIndex = primaryIndex[field], let rows = fieldIndex[value] else {
            return nil
        }
        return rows
    }
    
    // MARK: - Row Data
        
    /**Retrieves the data for a specific row. Works only when saved in active memory, this data is not saved in file
    
        - Parameter rowNumber: The row number to retrieve data for.
        - Returns: The `CSVRow` data for the specified row, or `nil` if not found.
    **/
    public func getFieldData(forRow rowNumber: Int) -> CSVRow? {
        return rowData[rowNumber]
    }

    /** Retrieves rows that have similar values to a specified value within a given threshold (Levenshtein distance).
        
         - Parameters:
            - field: The field name to check for similar values.
            - value: The value to compare against.
            - threshold: The maximum allowed Levenshtein distance for matching values.
         - Returns: An array of row numbers with similar values to the given value under the specified field.
    **/
    public func getRowsWithSimilarValues(forField field: String, value: String, threshold: Int = 2) -> [Int]? {
        guard let fieldIndex = primaryIndex[field] else {
            return nil
        }
        var matchingRows: [Int] = []
        
        for (fieldValue, rows) in fieldIndex {
            if levenshteinDistance(fieldValue, value) <= threshold {
                matchingRows.append(contentsOf: rows)
            }
        }
        return matchingRows
    }

    /// Calculates the Levenshtein distance between two strings.
    private func levenshteinDistance(_ str1: String, _ str2: String) -> Int {
        let m = str1.count
        let n = str2.count
        var matrix = Array(repeating: Array(repeating: 0, count: n + 1), count: m + 1)
        
        for i in 0...m {
            matrix[i][0] = i
        }
        for j in 0...n {
            matrix[0][j] = j
        }
        
        for i in 1...m {
            for j in 1...n {
                let cost = (str1[str1.index(str1.startIndex, offsetBy: i - 1)] == str2[str2.index(str2.startIndex, offsetBy: j - 1)]) ? 0 : 1
                matrix[i][j] = min(matrix[i - 1][j] + 1, matrix[i][j - 1] + 1, matrix[i - 1][j - 1] + cost)
            }
        }
        return matrix[m][n]
    }

    /** Finds the field with the most duplicate values in the primary index.
        
        This method iterates through all the fields in the primary index and counts how many duplicates are present for each field.
        It returns the field with the highest number of duplicates, along with the count of those duplicates.
        
        - Returns: a tuple containing the field name (String) with the most duplicates and the number of duplicates (Int), or `nil` if no duplicates are found.
    **/
    public func findFieldWithMostDuplicates() -> (String, Int)? {
        var maxDuplicates: Int = 0
        var fieldWithMaxDuplicates: String?
        
        for (field, fieldIndex) in primaryIndex {
            var duplicateCount = 0
            for (_, rows) in fieldIndex {
                if rows.count > 1 {
                    duplicateCount += 1
                }
            }
            if duplicateCount > maxDuplicates {
                maxDuplicates = duplicateCount
                fieldWithMaxDuplicates = field
            }
        }
        
        if let field = fieldWithMaxDuplicates {
            return (field, maxDuplicates)
        } else {
            return nil
        }
    }

    public func getRowByID(_ rowID: Int) -> CSVRow? {
        return rowData[rowID]
    }
  
    public func saveToFile() {
        let fileManager = FileManager.default
        guard let documentDirectory = fileManager.urls(for: .documentDirectory, in: .userDomainMask).first else {
            print("Could not find document directory.")
            return
        }
        
        let indexFileURL = documentDirectory.appendingPathComponent("csvIndex.json")
        
        do {
            let encoder = JSONEncoder()
            let data = try encoder.encode(primaryIndex)
            try data.write(to: indexFileURL)
            print("Index saved to \(indexFileURL.path)")
        } catch {
            print("Error saving index to file: \(error)")
        }
    }

    /** Loads the primary index from a file in the document directory.
        This method attempts to read a JSON file (`csvIndex.json`) from the document directory and deserializes it into the `primaryIndex`.
        If the file exists and is correctly formatted, the method loads the index and returns `true`; otherwise, it returns `false`.
        
        - Returns: a Boolean value indicating whether the index was successfully loaded (`true`) or not (`false`).
    **/
    public func loadFromFile() -> Bool {
        let fileManager = FileManager.default
        guard let documentDirectory = fileManager.urls(for: .documentDirectory, in: .userDomainMask).first else {
            print("Could not find document directory.")
            return false
        }
        
        let indexFileURL = documentDirectory.appendingPathComponent("csvIndex.json")
        
        print("Attempting to load index from: \(indexFileURL.path)")
        
        if fileManager.fileExists(atPath: indexFileURL.path) {
            do {
                let data = try Data(contentsOf: indexFileURL)
                let decoder = JSONDecoder()
                primaryIndex = try decoder.decode([String: [String: [Int]]].self, from: data)
                print("Index loaded successfully.")
                return true
            } catch {
                print("Error loading index from file: \(error)")
                return false
            }
        } else {
            print("Index file not found at \(indexFileURL.path).")
            return false
        }
    }
    
    /** Queries the index for a specific field with a given condition.
        
        This method allows querying the primary index for a specific field with a condition. The condition can be an equality check,
        a comparison for less than, greater than, or a range check. It returns the list of row IDs that satisfy the condition.
        
        - Parameters:
            -  field: The field to query in the index.
            - condition: A `QueryCondition` object that defines the condition for the query.
        - Returns: An array of row IDs (`[Int]`) that match the condition, or `nil` if the field or condition is invalid.
    **/
    public func queryIndex(for field: String, withCondition condition: QueryCondition) -> [Int]? {
        guard let fieldIndex = primaryIndex[field] else {
            return nil
        }

        switch condition.operator {
        case .equals:
            return fieldIndex[condition.value]
            
        case .lessThan:
            return fieldIndex.keys
                .filter { $0 < condition.value }
                .compactMap { fieldIndex[$0] }
                .flatMap { $0 }
            
        case .greaterThan:
            return fieldIndex.keys
                .filter { $0 > condition.value }
                .compactMap { fieldIndex[$0] }
                .flatMap { $0 }
            
        case .range:
            guard let lowerLimit = condition.range?.0, let upperLimit = condition.range?.1 else {
                return nil
            }
            return fieldIndex.keys
                .filter { $0 >= lowerLimit && $0 <= upperLimit }
                .compactMap { fieldIndex[$0] }
                .flatMap { $0 }
        }
    }


    
    public func saveRowPositionsToFile() {
        let fileManager = FileManager.default
        guard let documentDirectory = fileManager.urls(for: .documentDirectory, in: .userDomainMask).first else {
            print("Could not find document directory.")
            return
        }
        
        let rowPositionsFileURL = documentDirectory.appendingPathComponent("rowPositions.json")
        
        do {
            let encoder = JSONEncoder()
            let data = try encoder.encode(rowPositions)
            try data.write(to: rowPositionsFileURL)
            print("Row positions saved to \(rowPositionsFileURL.path)")
        } catch {
            print("Error saving row positions to file: \(error)")
        }
    }
    
    /** Loads the positions of rows from a file in the document directory.
        
        This method attempts to read a JSON file (`rowPositions.json`) from the document directory and deserializes it into the `rowPositions`.
        If the file exists and is correctly formatted, the method loads the row positions and returns `true`; otherwise, it returns `false`.
        
        - Returns: A Boolean value indicating whether the row positions were successfully loaded (`true`) or not (`false`).
    **/
    public func loadRowPositionsFromFile() -> Bool {
        let fileManager = FileManager.default
        guard let documentDirectory = fileManager.urls(for: .documentDirectory, in: .userDomainMask).first else {
            print("Could not find document directory.")
            return false
        }
        
        let rowPositionsFileURL = documentDirectory.appendingPathComponent("rowPositions.json")
        
        print("Attempting to load row positions from: \(rowPositionsFileURL.path)")
        
        if fileManager.fileExists(atPath: rowPositionsFileURL.path) {
            do {
                let data = try Data(contentsOf: rowPositionsFileURL)
                let decoder = JSONDecoder()
                rowPositions = try decoder.decode([Int: UInt64].self, from: data)
                print("Row positions loaded successfully.")
                return true
            } catch {
                print("Error loading row positions from file: \(error)")
                return false
            }
        } else {
            print("Row positions file not found at \(rowPositionsFileURL.path).")
            return false
        }
    }

    public func allRowIDs() -> [Int] {
        return Array(rowData.keys)
    }


}
