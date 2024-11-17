import Foundation

/// struct to hold the parsed rows
public struct CSVRow {
    var data: [String: Any]
    var isDuplicate: Bool = false 

    init(data: [String: Any]) {
        self.data = data
    }
}

/// A parser for reading and processing CSV files.
/// Handles query searching
@available(iOS 13.0.0, *)
public class CSVParser {
    public var headers: [String] = []
    private var filePath: String
    private var fileHandle: FileHandle?
    private var currentOffset: UInt64 = 0
    private var chunkSize: Int = 4096
    public var indexManager: IndexManager
    private var rowCounter: Int = 2

    /// Initializes a new CSVParser instance.
    /// - Parameters:
    ///   - filePath: The path to the CSV file, that user wants to proceed
    ///  Initializes fileHandle and indexManager instance for creating inverted index
    public init(filePath: String) {
        self.filePath = filePath
        self.fileHandle = FileHandle(forReadingAtPath: filePath)
        self.indexManager = IndexManager()

    }
    
    public func resetParsing() async {
        self.currentOffset = 0
        self.rowCounter = 0
    }

    /// Parses the headers of the CSV file.
    /// - is called for regular CSV file parsing
    /// - is called when user runs different queries for created inverted index, for mapping the values to correct headers (headers in index are saved in random order)
    public func parseHeaders() async {
        self.currentOffset = 0
        
        if let line = await readLineAsync() {
            
            let rawHeaders = line.split(separator: ",").map { String($0).trimmingCharacters(in: .whitespacesAndNewlines).lowercased() }
            
            if rawHeaders.count >= 2 {
                headers = rawHeaders
                print("Headers parsed: \(headers)")
            } else {
                print("Skipping invalid header line: \(line)")
            }
            currentOffset += UInt64(line.utf8.count + 1)
        }
    }

    func nextRowAsync() async -> CSVRow? {
        var line: String?
        
        repeat {
            line = await readLineAsync()
            
            let currentRowOffset = currentOffset
            
            guard let currentLine = line else { return nil }
            
            let values = parseCSVLine(currentLine)
            
            if values.count != headers.count {
                
                continue
            }
            
            var row = [String: Any]()
            for (index, value) in values.enumerated() where index < headers.count {
                let detectedValue = detectType(for: value)
                row[headers[index]] = detectedValue
                let field = headers[index]
                let csvRow = CSVRow(data: row)
                indexManager.addToIndex(field: field, value: value, rowNumber: rowCounter, row: csvRow)
            }
            indexManager.addRowPosition(rowID: rowCounter, position: currentRowOffset)
                    
            if rowCounter % 500 == 0 {
                indexManager.saveToFile()
                indexManager.saveRowPositionsToFile()
            }
            
            rowCounter += 1
            return CSVRow(data: row)
        } while line != nil
        
        return nil
    }

    func parseCSVLine(_ line: String) -> [String] {
        var values: [String] = []
        var currentValue = ""
        var inQuotes = false
        var previousChar: Character?

        for char in line {
            if char == "," && !inQuotes {
                values.append(currentValue.lowercased())
                currentValue = ""
            } else if char == "\"" {
                if previousChar == "\"" {
                    currentValue.append("\"")
                } else {
                    inQuotes.toggle()
                }
            } else {
                currentValue.append(char.lowercased())
            }
            previousChar = char
        }
        values.append(currentValue.lowercased())

        return values
    }

    /// Reads the next line from the CSV file asynchronously.
    /// - Returns: The next line as a `String`, or `nil` if no more lines are available.
    private func readLineAsync() async -> String? {
        return await withCheckedContinuation { continuation in
            DispatchQueue.global().async {
                guard let fileHandle = self.fileHandle else {
                    continuation.resume(returning: nil)
                    return
                }
                
                fileHandle.seek(toFileOffset: self.currentOffset)
                let data = fileHandle.readData(ofLength: self.chunkSize)
                
                guard !data.isEmpty, let chunkString = String(data: data, encoding: .utf8) else {
                    continuation.resume(returning: nil)
                    return
                }

                let lines = chunkString.split(whereSeparator: \.isNewline)
                if let line = lines.first {
                    self.currentOffset += UInt64(line.utf8.count + 1)
                    continuation.resume(returning: String(line))
                } else {
                    self.currentOffset += UInt64(data.count)
                    continuation.resume(returning: nil)
                }
            }
        }
    }

    private func detectType(for value: String) -> Any {
        if value.isEmpty { return "" }
        if let intValue = Int(value) { return intValue }
        if let doubleValue = Double(value) { return doubleValue }
        if let dateValue = parseDate(from: value) { return dateValue }
        return value
    }

    private func parseDate(from value: String) -> Date? {
        let dateFormats = ["yyyy-MM-dd", "MM/dd/yyyy", "yyyy/MM/dd"]
        let dateFormatter = DateFormatter()

        for format in dateFormats {
            dateFormatter.dateFormat = format
            if let date = dateFormatter.date(from: value) {
                return date
            }
        }
        return nil
    }

    public func processRowsAsync(batchSize: Int = 500) async {
        var rows: [CSVRow] = []
        var rowCount = 0
        
        while let row = await nextRowAsync() {
            rows.append(row)
            rowCount += 1
            
            if rowCount >= batchSize {
                await analyzeAndWriteResults(rows)
                rows.removeAll()
                rowCount = 0
            }
        }

        if !rows.isEmpty {
            await analyzeAndWriteResults(rows)
        }
    }

    private func analyzeAndWriteResults(_ rows: [CSVRow]) async {
        await withCheckedContinuation { continuation in
            DispatchQueue.global().async {
                let results = rows.map { self.analyzeRow($0) }
                self.writeResultsToFile(results)
                continuation.resume()
            }
        }
    }

    private func analyzeRow(_ row: CSVRow) -> String {
        return "Analysis result for \(row.data)"
    }

    private func writeResultsToFile(_ results: [String]) {
        print("Writing results: \(results)")
    }
    
    func readRow(at position: UInt64) -> CSVRow? {
        guard let fileHandle = self.fileHandle else { return nil }
        
        fileHandle.seek(toFileOffset: position)
        let data = fileHandle.readData(ofLength: chunkSize)
        
        guard let line = String(data: data, encoding: .utf8)?
                .split(separator: "\n")
                .first
        else { return nil }

        let values = line.split(separator: ",").map { String($0).trimmingCharacters(in: .whitespacesAndNewlines) }
        var row = [String: Any]()

        for (index, value) in values.enumerated() where index < headers.count {
            row[headers[index]] = detectType(for: value)
        }

        return CSVRow(data: row)
    }
    
    /**
    Asynchronously searches for rows in a CSV dataset that match the given query.

    This method parses the provided query string into search conditions, loads the index file, and searches for matching rows based on those conditions. Then it calles method **getRowByID** to find the rows with the according indexes. Then checks each returned row against the conditions and returns the results as a list of dictionaries where each dictionary represents a row's data.

    - Parameters:
        - query: A String representing the search query. The query is parsed to extract conditions that will be used to filter rows in the CSV dataset.

    - Returns:
        - A  [[String: Any]]?  (optional array of dictionaries), where each dictionary represents a row's data that matches the search conditions.

    - Example:
     Task {
         if let results = await parser.searchRowsAsync(query: "first name=dave and last name=moran") {
             for result in results {
                 print(result)
             }
         } else {
             print("No matching rows found.")
         }
     }
    **/
    @available(iOS 16.0, *)
    public func searchRowsAsync(query: String) async -> [[String: Any]]? {
        guard let conditions = SearchQueryParser.parse(query: query) else {
            print("Invalid query")
            return nil
        }
        
        var results: [CSVRow] = []
        var rowCount = 0
        
        if !indexManager.loadFromFile() {
            print("Failed to load index. Cannot perform search.")
            return nil
        }
        
        var matchingRowIDs: Set<Int> = Set()
        
        for condition in conditions {
            if let rowIDs = indexManager.queryIndex(for: condition.field, withCondition: condition) {
                let rowIDSet = Set(rowIDs)
                if matchingRowIDs.isEmpty {
                    matchingRowIDs = rowIDSet
                } else {
                    matchingRowIDs.formIntersection(rowIDSet)
                }
            } else {
                print("No matching rows found for field: \(condition.field), condition: \(condition)")
                return nil
            }
        }
        
        for rowID in matchingRowIDs {
            print("id \(rowID)")
            if let row = getRowByID(rowID) {
                var match = true
                
                for condition in conditions {
                    if let rowValue = row.data[condition.field] {
                        match = match && checkCondition(value: rowValue, condition: condition)
                    } else {
                        match = false
                        break
                    }
                }
                
                if match {
                    results.append(row)
                }
            }
            
            rowCount += 1
            
            if rowCount % 500 == 0 {
                await saveResultsToFile(results)
                results.removeAll()
            }
        }
        
        if !results.isEmpty {
            await saveResultsToFile(results)
        }
        
        let finalResults = results.map { $0.data }
        return finalResults
    }
    
    /**
    Checks whether a row's value for a specific field satisfies the condition.

    This method compares a value (either a `String`, `Double`, or `Date`) against the specified condition and its operator (e.g., equals, less than, greater than, range).

    - Parameters:
        - `value`: The value from the row that will be checked against the condition.
        - `condition`: The `QueryCondition` representing the search condition, including the field, operator, and value to check.

    - Returns:
        - A `Bool`, indicating whether the `value` satisfies the `condition`. Returns `true` if the condition is satisfied, otherwise `false`.
     **/
    private func checkCondition(value: Any, condition: QueryCondition) -> Bool {
        switch condition.operator {
        case .equals:
            if let stringValue = value as? String {
                return stringValue == condition.value
            } else if let dateValue = value as? Date, let conditionDate = parseDate(from: condition.value) {
                return dateValue == conditionDate
            }
            return false
            
        case .lessThan:
            if let numberValue = value as? Double, let limit = Double(condition.value) {
                return numberValue < limit
            } else if let dateValue = value as? Date, let limitDate = parseDate(from: condition.value) {
                return dateValue < limitDate
            }
            return false
            
        case .greaterThan:
            if let numberValue = value as? Double, let limit = Double(condition.value) {
                return numberValue > limit
            } else if let dateValue = value as? Date, let limitDate = parseDate(from: condition.value) {
                return dateValue > limitDate
            }
            return false
            
        case .range:
            if let numberValue = value as? Double,
               let lowerLimit = Double(condition.range?.0 ?? ""),
               let upperLimit = Double(condition.range?.1 ?? "") {
                return numberValue >= lowerLimit && numberValue <= upperLimit
            } else if let dateValue = value as? Date,
                      let lowerLimitDate = parseDate(from: condition.range?.0 ?? ""),
                      let upperLimitDate = parseDate(from: condition.range?.1 ?? "") {
                return dateValue >= lowerLimitDate && dateValue <= upperLimitDate
            }
            return false
        }
    }

    private func saveResultsToFile(_ results: [CSVRow]) async {
        await withCheckedContinuation { continuation in
            DispatchQueue.global().async {
                print("Saving results: \(results)")
                continuation.resume()
            }
        }
    }

    public func getRows(for value: String) -> [CSVRow]? {
        var rows: [CSVRow] = []
        
        for (_, index) in indexManager.primaryIndex {
            if let rowNumbers = index[value] {
                rows.append(contentsOf: rowNumbers.compactMap { readRow(at: UInt64($0)) })
            }
        }
        
        return rows.isEmpty ? nil : rows
    }
    
    public func getRowByID(_ rowID: Int) -> CSVRow? {
        if let cachedRow = indexManager.getCachedRow(rowID) {
            return cachedRow
        }
        
        if let loadedRow = loadRowFromCSV(rowID: rowID) {
            indexManager.cacheRow(rowID, row: loadedRow)
            return loadedRow
        }
        
        return nil
    }

    /** Loads a row from the CSV file given the row ID.
        This method retrieves a row from the CSV file based on the provided `rowID`. It searches for the row position in the index and reads the data at that position. The data is parsed, and the row is returned as a `CSVRow` object. If the row cannot be found or there are issues reading the data, it returns `nil`.
        - Parameters: rowID: The unique identifier for the row to be loaded.
    
        - Returns: A `CSVRow` object containing the parsed data for the specified row, or `nil` if the row cannot be loaded.
     **/
    private func loadRowFromCSV(rowID: Int) -> CSVRow? {
        print("in load")
        
        if headers.count == 0 {
            return nil
        }
        
        guard let rowPosition = indexManager.getRowPosition(rowID-1) else {
            print("Row position is not available in the index")
            return nil
        }
        
        guard let fileHandle = self.fileHandle else {
            print("File handle is not available")
            return nil
        }
        
        fileHandle.seek(toFileOffset: rowPosition)
        
        let chunkSize = 200
        let data = fileHandle.readData(ofLength: chunkSize)
        
        guard let line = String(data: data, encoding: .utf8)?
            .replacingOccurrences(of: "\r\n", with: "\n")
            .replacingOccurrences(of: "\r", with: "\n")
            .split(separator: "\n")
            .first else {
            print("Could not decode or split data")
            return nil
        }
        
        let values = parseCSVLine(String(line))
        guard values.count == indexManager.primaryIndex.count else {
            
            print("Row has incorrect number of values")
            return nil
        }
        
        var rowData = [String: Any]()
        for (index, value) in values.enumerated() {
            rowData[headers[index]] = detectType(for: value)
        }
        
        return CSVRow(data: rowData)
    }
}
