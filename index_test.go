package fulltext

import (
	"context"
	"reflect"
	"testing"
)

var (
	docOne = Doc{
		ID:   1,
		Text: "The quick brown fox jumps over the lazy dog",
	}
	docTwo = Doc{
		ID:   2,
		Text: "She sells sea shells by the sea shore",
	}
	docThree = Doc{
		ID:   3,
		Text: "Peter Piper picked a peck of pickled peppers while jumping over the sea shells",
	}
)

func TestService_Upsert(t *testing.T) {
	ctx := context.TODO()
	svc := NewService()
	err := svc.Upsert(ctx, []Doc{docThree})
	if err != nil {
		t.Fatal(err)
	}
	got, err := svc.Search(ctx, "pickled")
	if err != nil {
		t.Fatal(err)
	}
	if err != nil {
		t.Errorf("Service.Upsert() error = %v, wantErr %v", err, nil)
	}
	if len(got) != 1 || got[0] != docThree.ID {
		t.Errorf("Service.Upsert() = %v, want %v", got, []uint64{docThree.ID})
	}
	err = svc.Upsert(ctx, []Doc{{ID: docThree.ID, Text: "Peter Piper picked a peck of spicy peppers", PriorText: docThree.Text}})
	if err != nil {
		t.Fatal(err)
	}
	got, err = svc.Search(ctx, "pickled")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Errorf("Service.Upsert() = %v, want %v", got, []uint64{})
	}
	got, err = svc.Search(ctx, "spicy")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != docThree.ID {
		t.Errorf("Service.Upsert() = %v, want %v", got, []uint64{docThree.ID})
	}
}

func TestService_Search(t *testing.T) {
	testDocs := []Doc{
		docOne,
		docTwo,
		docThree,
	}
	ctx := context.TODO()
	svc := NewService()
	err := svc.Upsert(ctx, testDocs)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name    string
		svc     *Service
		query   string
		want    []uint64
		wantErr bool
	}{
		{
			name:    "'Fox' should match the quick brown fox in docOne",
			svc:     svc,
			query:   "Fox",
			want:    []uint64{docOne.ID},
			wantErr: false,
		},
		{
			name:    "'brow' should match as prefix of 'brown' in docOne",
			svc:     svc,
			query:   "brow",
			want:    []uint64{docOne.ID},
			wantErr: false,
		},
		{
			name:    "'row' should not match 'brown' in docOne because it is not a prefix of any word",
			svc:     svc,
			query:   "row",
			want:    []uint64{},
			wantErr: false,
		},
		{
			name:    "'row' should not match 'brown' in docOne because it is not a prefix of any word",
			svc:     svc,
			query:   "row",
			want:    []uint64{},
			wantErr: false,
		},
		{
			name:    "'picpep' should not match docThree because the suffix array filters out trigram false positives",
			svc:     svc,
			query:   "picpep",
			want:    []uint64{},
			wantErr: false,
		},
		{
			name:    "don't match unless both terms appear in the same doc",
			svc:     svc,
			query:   "fox sea",
			want:    []uint64{},
			wantErr: false,
		},
		{
			name:    "do match if both terms appear in the same doc",
			svc:     svc,
			query:   "She shore",
			want:    []uint64{docTwo.ID},
			wantErr: false,
		},
		{
			name:    "match as long as terms match prefixes of terms in the doc, without regard to case",
			svc:     svc,
			query:   "pET PiP",
			want:    []uint64{docThree.ID},
			wantErr: false,
		},
		{
			name:    "'jump' should match two docs",
			svc:     svc,
			query:   "jump",
			want:    []uint64{docOne.ID, docThree.ID},
			wantErr: false,
		},
		{
			name:    "empty query should return an error",
			svc:     svc,
			query:   "",
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.svc.Search(ctx, tt.query)
			if (err != nil) != tt.wantErr {
				t.Errorf("Service.Search() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Service.Search() = %v, want %v", got, tt.want)
			}
		})
	}
}
