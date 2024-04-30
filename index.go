// Package fulltext provides a fulltext indexing service using
// trigram indexing and suffix arrays.  It is designed for
// ASCII character sets but will attmept to transiliterare others.
package fulltext

import (
	"context"
	"fmt"
	"index/suffixarray"
	"strings"
	"sync"

	"github.com/dgryski/go-trigram"
	"github.com/nycmonkey/stringy"
)

const (
	saDelim = "\x00" // suffixArray delimiter; see https://eli.thegreenplace.net/2016/suffix-arrays-in-the-go-standard-library/
)

var (
	replacer = strings.NewReplacer(`-`, ` `, `_`, ` `, `:`, ` `, `|`, ` `)
)

// meta holds metadata about an indexed document
type meta struct {
	sa *suffixarray.Index // used to remove false positives from trigram index results
	id uint64             // external document ID
}

// Doc is a document to be indexed
type Doc struct {
	ID        uint64 // external ID not managed by the index.  It is the caller's responsibility to ensure uniqueness
	Text      string // the text to index
	PriorText string // the text that was previously indexed.  This is required for updates only - leave empty for new documents
}

// Service implements pb.FulltextServiceServer
type Service struct {
	docs         map[trigram.DocID]meta
	extIDs       map[uint64]trigram.DocID // tracks the IDs already in the index
	idx          trigram.Index            // allows lookup by name
	sync.RWMutex                          // protects docs and idx
}

// NewService initializes a fulltext index service
func NewService() *Service {
	return &Service{
		docs:   make(map[trigram.DocID]meta),
		extIDs: make(map[uint64]trigram.DocID),
		idx:    trigram.NewIndex(nil),
	}
}

// DocCount returns the number of documents in the index
func (svc *Service) DocCount() int {
	svc.RLock()
	defer svc.RUnlock()
	return len(svc.docs)
}

func analyze(text string) (tGrams []trigram.T, words []string) {
	words = stringy.Analyze(replacer.Replace(text))
	// prefix the start of each token with an underscore to ensure we only match from the beginning of words
	if len(words) == 0 {
		return
	}
	for i := 0; i < len(words); i++ {
		words[i] = `_` + words[i]
	}
	for _, tok := range words {
		tGrams = trigram.Extract(tok, tGrams)
	}
	return
}

// Search performs a fulltext search suitable for a typeahead search box.
// The returned docIDs are the external IDs provided at time of indexing.
func (svc *Service) Search(ctx context.Context, query string) (docIDs []uint64, err error) {
	tGrams, words := analyze(query)
	if len(tGrams) == 0 {
		err = fmt.Errorf(`query '%s' does not have enough content`, query)
		return
	}
	svc.RLock()
	defer svc.RUnlock()
	candidates := svc.idx.QueryTrigrams(tGrams)
	docIDs = make([]uint64, 0, len(candidates))
candidateLoop:
	for _, docID := range candidates {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		doc, ok := svc.docs[docID]
		if !ok {
			continue
		}
		for _, word := range words {
			if doc.sa.Lookup([]byte(word), 1) == nil {
				continue candidateLoop // false positive
			}
		}
		docIDs = append(docIDs, doc.id)
	}
	return
}

// Upsert adds or updates a document in the full text index
func (svc *Service) Upsert(ctx context.Context, docs []Doc) (err error) {
	// validate inputs
	for i, doc := range docs {
		if doc.ID == 0 {
			return fmt.Errorf(`docs[%d]: ID must be greater than zero`, i)
		}
		if _, ok := svc.extIDs[doc.ID]; ok {
			if len(doc.PriorText) == 0 {
				return fmt.Errorf(`docs[%d] is already indexed, but the OldText parameter was not provided.  To update the document, the text it contained previously must also be provided`, i)
			}
		}
	}
	// update the index
	var b strings.Builder
	svc.Lock()
	defer svc.Unlock()
	for _, doc := range docs {
		b.Reset()
		if docID, ok := svc.extIDs[doc.ID]; ok {
			// this is an update, so first remove the old document from the trigram index
			_, words := analyze(doc.PriorText)
			for _, word := range words {
				svc.idx.Delete(word, docID)
			}
			// now remove the metadata associated with the old internal ID
			delete(svc.docs, docID)
		}
		tGrams, words := analyze(doc.Text)
		for _, word := range words {
			b.WriteString(saDelim)
			b.WriteString(word)
		}
		b.WriteString(saDelim)
		docID := svc.idx.AddTrigrams(tGrams)
		svc.docs[docID] = meta{
			id: doc.ID,
			sa: suffixarray.New([]byte(b.String())),
		}
		svc.extIDs[doc.ID] = docID
	}
	svc.idx.Prune(0.1)
	svc.idx.Sort()
	return nil
}
