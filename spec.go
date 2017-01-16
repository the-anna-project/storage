package storage

import (
	"time"
)

// Backoff represents the object managing backoff algorithms to retry actions.
type Backoff interface {
	// NextBackOff provides the duration expected to wait before retrying an
	// action. time.Duration = -1 indicates that no more retry should be
	// attempted.
	NextBackOff() time.Duration
	// Reset sets the backoff back to its initial state.
	Reset()
}

// Service represents a persistency management object. Different storages may be
// provided by a storage collection. Within a receiver function the usage of the
// feature storage may look like this.
//
//     func (s *service) Foo() error {
//       rk, err := s.Storage.Feature.GetRandom()
//       ...
//     }
//
// // TODO document operations against data structures
//
//     keys
//     lists
//     scored sets
//     sets
//     strings
//
// TODO fix the naming
//
//     good
//
//         GetAllFromList
//
//     bad
//
//         Get (better GetValueFromKey)
//
type Service interface {
	// Boot initializes and starts the whole storage like booting a machine. The
	// call to Boot blocks until the storage is completely initialized, so you
	// might want to call it in a separate goroutine.
	Boot()
	// Exists checks whether a given key exists. The existence check does not rely
	// on a specific data structure type.
	Exists(key string) (bool, error)
	// ExistsInScoredSet checks whether a given element exists within the scored
	// set identified by the given key.
	ExistsInScoredSet(key, element string) (bool, error)
	// Get returns data associated with key. This is a simple key-value
	// relationship.
	Get(key string) (string, error)
	// GetAllFromList returns all elements from the list under key. This method
	// has an enormous time complexity. GetAllFromList should therefore never be
	// used against bigger lists, where a big list is characterized by a length
	// above 100.
	GetAllFromList(key string) ([]string, error)
	// GetAllFromSet returns all elements from the set under key.
	GetAllFromSet(key string) ([]string, error)
	// GetElementsByScore looks up all elements associated with the given score.
	// To limit the number of returned elements, maxElements ca be used. Note
	// that the list has this scheme.
	//
	//     element1,element2,...
	//
	GetElementsByScore(key string, score float64, maxElements int) ([]string, error)
	// GetHighestScoredElements searches a list that is ordered by their
	// element's score, and returns the elements and their corresponding scores,
	// where the highest scored element is the first in the returned list. Note
	// that the list has this scheme.
	//
	//     element1,score1,element2,score2,...
	//
	// Note that the resulting list will have the length of maxElements*2,
	// because the list contains the elements and their scores.
	//
	// TODO check for highest score and return truley random element if highest
	// score is 0
	GetHighestScoredElements(key string, maxElements int) ([]string, error)
	// GetRandom returns a random key which was formerly stored within the
	// underlying storage.
	GetRandom() (string, error)
	// GetRandomFromSet returns a random element which was formerly stored within
	// the set identified by the given key.
	GetRandomFromSet(key string) (string, error)
	// GetRandomFromScoredSet returns a random element which was formerly stored
	// within the scored set identified by the given key.
	GetRandomFromScoredSet(key string) (string, error)
	// GetScoreOfElement returns the score of the element within the scored set
	// identified by the given key. In case there does no element exist,
	// GetScoreOfElement returns a not found error.
	GetScoreOfElement(key, element string) (float64, error)
	// GetStringMap returns the hash map stored under the given key.
	GetStringMap(key string) (map[string]string, error)
	// Increment increments the floating point number stored under key by the
	// given value n.
	Increment(key string, n float64) (float64, error)
	// IncrementScoredElement increments the floating point number stored under
	// element within the scored set identified by key by the given value n.
	IncrementScoredElement(key, element string, n float64) (float64, error)
	// LengthOfList returns the number of elements within the list given by key.
	LengthOfList(key string) (int, error)
	// LengthOfScoredSet returns the number of elements within the scored set
	// given by key.
	LengthOfScoredSet(key string) (int, error)
	// PopFromList returns the next element from the list identified by the given
	// key. Note that a list is an ordered sequence of arbitrary elements.
	// PushToList and PopFromList are operating according to a "first in, first
	// out" primitive. If the requested list is empty, PopFromList blocks
	// infinitely until an element is added to the list. Returned elements will
	// also be removed from the specified list.
	PopFromList(key string) (string, error)
	// PushToList adds the given element to the list identified by the given key.
	// Note that a list is an ordered sequence of arbitrary elements. PushToList
	// and PopFromList are operating according to a "first in, first out"
	// primitive.
	PushToList(key string, element string) error
	// PushToSet adds the given element to the set identified by the given key.
	// Note that a set is an unordered collection of distinct elements.
	PushToSet(key string, element string) error
	// Remove deletes the given key.
	Remove(key string) error
	// RemoveFromList removes the given element from the list identified by the
	// given key.
	RemoveFromList(key string, element string) error
	// RemoveFromSet removes the given element from the set identified by the
	// given key.
	RemoveFromSet(key string, element string) error
	// RemoveScoredElement removes the given element from the scored set under
	// key.
	RemoveScoredElement(key string, element string) error
	// Set stores the given key value pair. Once persisted, value can be
	// retrieved using Get.
	Set(key string, value string) error
	// SetElementByScore persists the given element in the weighted list
	// identified by key with respect to the given score.
	SetElementByScore(key, element string, score float64) error
	// SetStringMap stores the given stringMap under the given key.
	SetStringMap(key string, stringMap map[string]string) error
	// Shutdown ends all processes of the storage like shutting down a machine.
	// The call to Shutdown blocks until the storage is completely shut down, so
	// you might want to call it in a separate goroutine.
	Shutdown()
	// TrimEndOfList cuts the tail of the list identified by the given key to
	// ensure that there are not more elements within the list than defined by
	// maxElements.
	TrimEndOfList(key string, maxElements int) error
	// WalkSet scans the set given by key and executes the callback for each found
	// element.
	//
	// The walk is throttled. That means some amount of elements are fetched at
	// once from the storage. After all fetched elements are iterated, the next
	// batch of elements is fetched to continue the next iteration, until the
	// given set is walked completely. The given closer can be used to end the
	// walk immediately.
	WalkSet(key string, closer <-chan struct{}, cb func(element string) error) error
	// WalkKeys scans the key space with respect to the given glob and executes
	// the callback for each found key.
	//
	// The walk is throttled. That means some amount of keys are fetched at once
	// from the storage. After all fetched keys are iterated, the next batch of
	// keys is fetched to continue the next iteration, until the whole key space
	// is walked completely. The given closer can be used to end the walk
	// immediately.
	WalkKeys(glob string, closer <-chan struct{}, cb func(key string) error) error
	// WalkScoredSet scans the scored set given by key and executes the callback
	// for each found element. Note that the walk might ignores the score order.
	//
	// The walk is throttled. That means some amount of elements are fetched at
	// once from the storage. After all fetched elements are iterated, the next
	// batch of elements is fetched to continue the next iteration, until the
	// given set is walked completely. The given closer can be used to end the
	// walk immediately.
	WalkScoredSet(key string, closer <-chan struct{}, cb func(element string, score float64) error) error
}
