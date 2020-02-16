/*
Copyright 2019 Tamás Gulácsi

// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0
*/

package genocall

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestParseArgDocs(t *testing.T) {
	for _, tC := range []struct {
		Name, In string
		Want     argDocs
	}{
		{
			Name: "KO",
			In: `  dupla_bejelentes
    - KÖV03352 - A dupla bejelentések kiszűrésére a COREWEB jelenleg bekérdez FairKár-ba  (FairKár procedure dupla_bejelentes_PUBWEB eljárás hívás)

    Egy bejelentést akkor tekintünk dupla bejelentésnek GFB esetében,
      ha minden bemenő paraméter (beleértve a károsult és a károkozó rendszámát, valamint a bejelentő státuszát is) megegyezik egy már létező kár paramétereivel.
      Ebben a tekintetben a károsult, javító és partner ugyanannak a bejelentő státusznak tekintendő.
      Azaz ugyanazt a kárt bejelentheti a károsult és a károkozó is, ez nem tekintendő dupla bejelentésnek, de ha pl. javító és károsult jelenti be ugyanazt a kárt (többi paraméter ugyanaz), az dupla bejelentésnek minősül.
    Egy bejelentést akkor tekintünk dupla bejelentésnek CASCO és GAP esetében, ha a
        	Kártípus
        	Kárdátum
        	Károsult rendszám
      megegyezik (ezekben az esetekben károsult = károkozó, így külön károsulti és károkozói bejelentés nem értelmezett).

    Több károsult, egy károkozó GFB esetében lehetséges abban az esetben, ha az adott
      	kárdátummal,
      	kártípussal és
      	károkozó rendszámmal
    létezik már kár a Brunoban, de más a károsult rendszám (egy autó több másik gépjárműben okozott kárt).

    Több károsult, egy károkozó CASCO és GAP esetében nem releváns (visszaadott érték: nem).

    in:
      - p_tipus - VC(10) - bejelentés típusa: 1/GFB/2/CASCO/4/GAP
      - p_bejelento - VC(10) - UGYFEL/OKOZO/PARTNER/JAVITO (Bejelentő státusza (károsult, javító, partner,okozó))
      - p_karido - DATE - Kárdátum
      - p_karosult_rendszam - VC(11) - Károsult rendszám
      - p_okozo_rendszam - VC(11) - Károkozó rendszám

    out:
      - p_dupla_bejelentes - dupla bejelentés? I/N
      - p_tobb_kaorsult - több károsult? I/N

    return:
      - hibakód - 0: rendben; negatív: hibakód
`,
			Want: argDocs{
				Pre: `
    in:
      - p_tipus - VC(10) - bejelentés típusa: 1/GFB/2/CASCO/4/GAP
      - p_bejelento - VC(10) - UGYFEL/OKOZO/PARTNER/JAVITO (Bejelentő státusza (károsult, javító, partner,okozó))
      - p_karido - DATE - Kárdátum
      - p_karosult_rendszam - VC(11) - Károsult rendszám
      - p_okozo_rendszam - VC(11) - Károkozó rendszám

    out:
      - p_dupla_bejelentes - dupla bejelentés? I/N
      - p_tobb_kaorsult - több károsult? I/N

    return:
      - hibakód - 0: rendben; negatív: hibakód
`,
				Post: `
    out:
      - p_dupla_bejelentes - dupla bejelentés? I/N
      - p_tobb_kaorsult - több károsult? I/N

    return:
      - hibakód - 0: rendben; negatív: hibakód
`,
				Map: map[string]string{
					"p_tipus":             "VC(10) - bejelentés típusa: 1/GFB/2/CASCO/4/GAP",
					"p_bejelento":         "VC(10) - UGYFEL/OKOZO/PARTNER/JAVITO (Bejelentő státusza (károsult, javító, partner,okozó))",
					"p_karido":            "DATE - Kárdátum",
					"p_karosult_rendszam": "VC(11) - Károsult rendszám",
					"p_okozo_rendszam":    "VC(11) - Károkozó rendszám",
				},
			},
		},
	} {
		var got argDocs
		got.Parse(tC.In)
		if diff := cmp.Diff(got, tC.Want); diff != "" {
			t.Errorf("%s.\ngot %+v,\nwanted %+v\ndiff %s", tC.Name, got, tC.Want, diff)
		}
	}
}
