/*
Copyright 2019 Tamás Gulácsi

// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0
*/

package genocall

import (
	"regexp"
	"strings"
)

var (
	rBegInput  = regexp.MustCompile("\n\\s*(?:- )?in(?:put)?:? *\n")
	rBegOutput = regexp.MustCompile("\n\\s*(?:(?:- )?out(?:put)?|ret(?:urns?)?):? *\n")
)

func splitDoc(doc string) (common, input, output string) {
	if doc = trimStartEmptyLines(doc); doc == "" {
		return common, input, output
	}
	common = doc
	ii := rBegOutput.FindStringIndex(doc)
	if ii != nil {
		doc, output = doc[:ii[0]], doc[ii[1]:]
	}
	if ii = rBegInput.FindStringIndex(doc); ii != nil {
		common, input = doc[:ii[0]], doc[ii[1]:]
	}
	return common, input, output
}

func splitByOffset(doc string) []string {
	if doc = trimStartEmptyLines(doc); doc == "" {
		return nil
	}
	var last, pos int
	lastOff := firstNotSpace(doc)
	var parts []string
	for _, line := range strings.SplitAfter(doc, "\n") {
		actOff := firstNotSpace(line)
		if lastOff >= actOff {
			if pos > last {
				parts = append(parts, doc[last:pos])
			}
			last = pos
		}
		pos += len(line)
	}
	if pos > last {
		parts = append(parts, doc[last:pos])
	}
	return parts
}

// trimStartEmptyLines the starting empty lines, keep the spaces (offset)
func trimStartEmptyLines(doc string) string {
	if doc = strings.TrimRight(doc, " \n\t"); doc == "" {
		return ""
	}
	if i := firstNotSpace(doc); i >= 0 {
		if j := strings.LastIndexByte(doc[:i], '\n'); j >= 0 {
			return doc[j+1:]
		}
	}
	return doc
}

func getDirDoc(doc string, dirmap direction) argDocs {
	var D argDocs
	common, input, output := splitDoc(doc)
	D.Pre = common
	if dirmap == DIR_IN {
		D.Parse(input)
	} else {
		D.Parse(output)
	}
	return D
}
