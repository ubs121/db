package triple

import (
	"strings"

	"github.com/ubs121/strings"
)

func getId(str string) (*string, string) {
	if len(str) == 0 {
		return nil, str
	}
	if str[0] == '<' {
		return getUriPart(str[1:])
	} else if str[0] == '"' {
		return getQuotedPart(str[1:])
	} else {

		// Technically not part of the spec. But we do it anyway for convenience.
		return getUnquotedPart(str)
	}
}

func getValue(str string) (*string, string) {
	if len(str) == 0 {
		return nil, str
	}

	if str[0] == '<' { // url
		return getUriPart(str[1:])
	} else if str[0] == '`' { // start of multiline
		return getMultilinePart(str[1:])
	} else if str[0] == '"' { // quoted
		return getQuotedPart(str[1:])
	} else {
		// TODO: тоо эсэхийг шалгах

		// Technically not part of the spec. But we do it anyway for convenience.
		return getUnquotedPart(str)
	}
}

func getUriPart(str string) (*string, string) {
	i := 0
	for i < len(str) && str[i] != '>' {
		i += 1
	}
	if i == len(str) {
		return nil, str
	}
	part := str[0:i]
	return &part, str[i+1:]
}

func getQuotedPart(str string) (*string, string) {
	i := 0
	start := 0
	out := ""
	for i < len(str) && str[i] != '"' {
		if str[i] == '\\' {
			out += str[start:i]
			switch str[i+1] {
			case '\\':
				out += "\\"
			case 'r':
				out += "\r"
			case 'n':
				out += "\n"
			case 't':
				out += "\t"
			case '"':
				out += "\""
			default:
				// Дан \ тэмдэгт оруулж ашиглаж магадгүй!
				out += "\\"
				//return nil, str
			}
			i += 2
			start = i
			continue
		}
		i += 1
	}

	if i == len(str) {
		return nil, str
	}
	out += str[start:i]
	i += 1
	var remainder string
	if strings.HasPrefix(str[i:], "^^<") {
		// Ignore type, for now
		_, remainder = getUriPart(str[i+3:])
	} else if strings.HasPrefix(str[i:], "@") {
		_, remainder = getUnquotedPart(str[i+1:])
	} else {
		remainder = str[i:]
	}

	return &out, remainder
}

func getMultilinePart(str string) (*string, string) {
	i := 0
	start := 0
	out := ""
	for i < len(str) && str[i] != '`' {
		if str[i] == '\\' {
			out += str[start:i]
			switch str[i+1] {
			case '\\':
				out += "\\"
			case 'r':
				out += "\r"
			case 'n':
				out += "\n"
			case 't':
				out += "\t"
			case '"':
				out += "\""
			default:
				// Дан \ тэмдэгт оруулж ашиглаж магадгүй!
				out += "\\"
				//return nil, str
			}
			i += 2
			start = i
			continue
		}
		i += 1
	}

	if i == len(str) {
		return nil, str
	}
	out += str[start:i]

	return &out, ""
}

func getUnquotedPart(str string) (*string, string) {
	i := 0
	initStr := str
	out := ""
	start := 0
	for i < len(str) && !isWhitespace(str[i]) {
		if str[i] == '"' {
			part, remainder := getQuotedPart(str[i+1:])
			if part == nil {
				return part, initStr
			}
			out += str[start:i]
			str = remainder
			i = 0
			start = 0
			out += *part
		}
		i += 1
	}
	out += str[start:i]
	return &out, str[i:]
}

func getTripleComponent(str string) (*string, string) {
	if len(str) == 0 {
		return nil, str
	}
	if str[0] == '<' {
		return getUriPart(str[1:])
	} else if str[0] == '`' {
		return getQuotedPart(str[1:])
	} else if str[0] == '"' {
		return getQuotedPart(str[1:])
	} else {
		// Technically not part of the spec. But we do it anyway for convenience.
		return getUnquotedPart(str)
	}
	/*else if str[0] == '.' {
		return nil, str
	} */
}
