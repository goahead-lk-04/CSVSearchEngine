//
//  SearchQueryParser.swift
//  CVSSearchEngineApp
//
//  Created by Lisa Kuchyna on 2024-11-15.
//

import Foundation

public enum QueryOperator {
    case equals
    case lessThan
    case greaterThan
    case range
}

public struct QueryCondition {
    var field: String
    var `operator`: QueryOperator
    var value: String
    var range: (String, String)?
}

/// Class to handle query parsing
@available(iOS 16.0, *)
@available(iOS 16.0, *)
@available(iOS 16.0, *)
public class SearchQueryParser {
    
    
    static func parse(query: String) -> [QueryCondition]? {
        var conditions: [QueryCondition] = []
        
        let parts = query.lowercased().split(separator: "and")
        for part in parts {
            let trimmedPart = part.trimmingCharacters(in: .whitespacesAndNewlines)
            if trimmedPart.contains("<") {
                let components = trimmedPart.split(separator: "<")
                if components.count == 2 {
                    conditions.append(QueryCondition(field: String(components[0]).trimmingCharacters(in: .whitespaces), operator: .lessThan, value: String(components[1]).trimmingCharacters(in: .whitespaces), range: nil))
                }
            } else if trimmedPart.contains(">") {
                let components = trimmedPart.split(separator: ">")
                if components.count == 2 {
                    conditions.append(QueryCondition(field: String(components[0]).trimmingCharacters(in: .whitespaces), operator: .greaterThan, value: String(components[1]).trimmingCharacters(in: .whitespaces), range: nil))
                }
            } else if trimmedPart.contains("=") {
                let components = trimmedPart.split(separator: "=")
                if components.count == 2 {
                    conditions.append(QueryCondition(field: String(components[0]).trimmingCharacters(in: .whitespaces), operator: .equals, value: String(components[1]).trimmingCharacters(in: .whitespaces), range: nil))
                }
            } else if trimmedPart.contains("..") {
                let components = trimmedPart.split(separator: "..")
                if components.count == 3 { // Expecting "field..lower..upper"
                    let field = String(components[0]).trimmingCharacters(in: .whitespaces)
                    let lowerLimit = String(components[1]).trimmingCharacters(in: .whitespaces)
                    let upperLimit = String(components[2]).trimmingCharacters(in: .whitespaces)
                    
                    conditions.append(QueryCondition(
                        field: field,
                        operator: .range,
                        value: "",
                        range: (lowerLimit, upperLimit)
                    ))
                }
            }

        }
        
        return conditions.isEmpty ? nil : conditions
    }

}
