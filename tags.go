package db

import (
	"reflect"
	"regexp"
	"strings"
)

var (
	rxTagSep = regexp.MustCompile(`[\s,\.\r\n\t\(\)/\:\"”“]`)

	// @see :http://mn.wikipedia.org/wiki/%D2%AE%D0%B9%D0%BB_%D2%AF%D0%B3)
	verbSuffix = []string{
		// тодорхой бус цаг
		"ах", "эх", "ох", "өх", "ух", "үх", "их",
		//одоо цагаар төгсгөх
		"на", "нэ", "но", "нө",
		// шийдэн хүсэх
		"ъя", "ье", "ъё",
		// өнгөрсөн цагаар төгсгөх
		"жээ", "чээ", "лаа", "лээ", "лоо", "лөө",
		// урьдчилан холбох
		"аад", "ээд", "оод", "өөд",
		// үгүйсгэн холбох
		"гүй",
		// хамтрах
		"тай", "тэй", "той",
		// чиглэх
		"руу", "рүү", "луу", "лүү",
		// гарах
		"аас", "ээс", "оос", "өөс",
		// үйлдэх
		"аар", "ээр", "оор", "өөр",
		// ердийн үргэлжлэх
		"даг", "дэг", "дог", "дөг",
		// сан(сэн, сон, сөн) - байх хэрэгтэй
		// болзон холбох
		"вал", "бал", "бэл", "бол", "бөл",
		// шаардан захирах
		"аач", "ээч", "ооч", "өөч",
		// угтан холбох
		"тал", "тэл", "тол", "төл",
		// бусдаар үйлдүүлэх хэв
		"лга", "лгэ", "лго", "лгө",
		// урьдчилан холбох
		"аад", "ээд", "оод", "өөд",
		// бүрмөсөн үйлдэх байдал
		"чих",
		// дашрамдан холбох
		"нгаа", "нгээ", "нгоо", "нгөө",
		// бэлтгэн холбох
		"магц", "мэгц", "могц", "мөгц",
		// олноороо үйлдэх
		"цгаа", "цгээ", "цгоо", "цгөө",
		// дагалдан холбох
		"хлаар", "хлээр", "хлоор", "хлөөр",
		// далимдан холбох
		"нгуут", "нгүүт",
		// зөвлөн захирах
		"аарай", "ээрэй", "оорой", "өөрэй",
		// мөрөөдөн хүсэх
		"аасай", "ээсэй", "оосой", "өөсөй"}
)

/// bson.M обектын tags талбарыг шинэчилнэ
/// бүх талбараар tag үүсгэх нь зөв үү?
func updateTags(obj M) {
	tags := []string{}

	for _, v := range obj {
		// зөвхөн string талбар дээр tag үүсгэнэ
		if v != nil && reflect.TypeOf(v).Name() == "string" {
			tags = mergeSlice(tags, extractTags(v.(string)))
		}
	}

	obj["tags"] = tags
}

// tags залгах, давхардлыг хасах
func mergeSlice(oldTags []string, newTags []string) []string {

	//sort.Strings(s1)

	for i := 0; i < len(newTags); i++ {
		pos := 0
		for pos < len(oldTags) && oldTags[pos] < newTags[i] {
			pos++
		}

		if pos < len(oldTags) {
			if newTags[i] == oldTags[pos] {
				// already exists, so skip it
			} else {
				// insert at pos
				oldTags = append(oldTags[:pos], append([]string{newTags[i]}, oldTags[pos:]...)...)
			}
		} else {
			// append it
			oldTags = append(oldTags, newTags[i])
		}

	}

	return oldTags
}

// боломжит түлхүүр үгүүдийг ялгана, үйл үгүүдийг хасна
func extractTags(str string) []string {
	var tags []string

	vals := rxTagSep.Split(strings.ToLower(str), -1)

	if len(str) < 60 { // (30 in utf-8) short text, just use it as tags
		for _, s := range vals {
			if len(s) > 4 { // 4 in bytes
				tags = append(tags, strings.ToLower(s))
			}
		}
	} else {
		for _, s := range vals {
			if len(s) > 4 && !isVerb(s) {
				// TODO: хувирсан үг бол язгуур үгийг олох. жнь: кодыг -> код
				tags = append(tags, s)
			}
		}
	}

	return tags
}

func isVerb(w string) bool {
	for i := 0; i < len(verbSuffix); i++ {
		if strings.HasSuffix(w, verbSuffix[i]) {
			return true
		}
	}
	return false
}
