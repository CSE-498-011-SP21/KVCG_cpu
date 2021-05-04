#ifndef __HOPSCOTCH_HASHMAP__
#define __HOPSCOTCH_HASHMAP__
////////////////////////////////////////////////////////////////////////////////
// ConcurrentHopscotchHashMap Class
//
////////////////////////////////////////////////////////////////////////////////
//TERMS OF USAGE
//------------------------------------------------------------------------------
//
//	Permission to use, copy, modify and distribute this software and
//	its documentation for any purpose is hereby granted without fee,
//	provided that due acknowledgments to the authors are provided and
//	this permission notice appears in all copies of the software.
//	The software is provided "as is". There is no warranty of any kind.
//
//Authors:
//	Maurice Herlihy
//	Brown University
//	and
//	Nir Shavit
//	Tel-Aviv University
//	and
//	Moran Tzafrir
//	Tel-Aviv University
//
//	Date: July 15, 2008.  
//
////////////////////////////////////////////////////////////////////////////////
// Programmer : Moran Tzafrir (MoranTza@gmail.com)
//
////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////
// INCLUDE DIRECTIVES
////////////////////////////////////////////////////////////////////////////////
#include <stdio.h>
#include <limits.h>
#include <sys/types.h>
#include <malloc.h>
#include <pthread.h>
#include <sched.h>
#include <semaphore.h>
#include <alloca.h>
#include <strings.h>
#include <string.h>
#include <shared_mutex>
#include <mutex>
#include "common.h"
#include <vector>
#include <functional>
#include <iostream>
#include <math.h>
#include <atomic>
#include "HopscotchWrapper.cpp"

using namespace std;

typedef unsigned char      _u8;
typedef unsigned short     _u16;
typedef unsigned int       _u32;
typedef unsigned long long _u64;

#define CACHE_LINE_SIZE 64
#define inline_ __inline__
#define ALIGNED_MALLOC(_s,_a)		memalign(_a, _s)
#define ALIGNED_FREE(_p)			free(_p)

//compare and set 
//////////////////////////////////////////////////////////////////////////
#define CAS32(_a,_o,_n)		__sync_val_compare_and_swap ((_u32*)_a,(_u32)_o,(_u32)_n)
#define CAS64(_a,_o,_n)		__sync_val_compare_and_swap ((_u64*)_a,(_u64)_o,(_u64)_n)
#define CASPO(_a,_o,_n)		__sync_val_compare_and_swap ((void**)_a,(void*)_o,(void*)_n)

#define SWAP32(_a,_n)		__sync_lock_test_and_set(_a, _n)
#define SWAP64(_a,_n)		__sync_lock_test_and_set(_a, _n)
#define SWAPPO(_a,_n)		__sync_lock_test_and_set(_a, _n)


////////////////////////////////////////////////////////////////////////////////
/*
 * II. Memory barriers. 
 *  WMB(): All preceding write operations must commit before any later writes.
 *  RMB(): All preceding read operations must commit before any later reads.
 *  MB():  All preceding memory accesses must commit before any later accesses.
 * 
 *  If the compiler does not observe these barriers (but any sane compiler
 *  will!), then VOLATILE should be defined as 'volatile'.
 */

#define MB()  __asm__ __volatile__ (";; mf ;; " : : : "memory")
#define WMB() MB()
#define RMB() MB()
#define VOLATILE /*volatile*/

// Internal Classes
class Memory {
public:
    static const int CACHELINE_SIZE = CACHE_LINE_SIZE;

    inline_ static void* byte_malloc(const size_t size)	{return malloc(size);}
    inline_ static void  byte_free(void* mem)					{free(mem);}

    inline_ static void* byte_aligned_malloc(const size_t size) {return ALIGNED_MALLOC(size, CACHELINE_SIZE);}
    inline_ static void* byte_aligned_malloc(const size_t size, const size_t alignment) {return ALIGNED_MALLOC(size,alignment);}
    inline_ static void  byte_aligned_free(void* mem)	{ALIGNED_FREE(mem);}

    inline_ static void  read_write_barrier()	{ MB();	}
    inline_ static void  write_barrier()		{ WMB(); }
    inline_ static void  read_barrier()			{ RMB(); }

    inline_ static _u32  compare_and_set(_u32 volatile* _a, _u32 _o, _u32 _n)		{return CAS32(_a,_o,_n);}
    inline_ static void* compare_and_set(void* volatile * _a, void* _o, void* _n)	{return CASPO(_a,_o,_n);}
    inline_ static _u64  compare_and_set(_u64 volatile* _a, _u64 _o, _u64 _n)		{return CAS64(_a,_o,_n);}

    inline_ static _u32  exchange_and_set(_u32 volatile *   _a, _u32  _n)	{return SWAP32(_a,_n);}
    inline_ static void* exchange_and_set(void * volatile * _a, void* _n)	{return SWAPPO(_a,_n);}
};

class HASH_INT {
public:
	//you must define the following fields and properties
	static const unsigned int _EMPTY_HASH;
	static const unsigned int _BUSY_HASH;
	static const int _EMPTY_KEY;
	static const int _EMPTY_DATA;

    //to help with wraper class add 
    static const int _ADD_SUCCESS;
    static const int _SIZE_INCREASED;

	inline_ static bool IsEqual(int left_key, int right_key) {
		return left_key == right_key;
	}

	inline_ static void relocate_key_reference(int volatile& left, const int volatile& right) {
		left = right;
	}

	inline_ static void relocate_data_reference(int volatile& left, const int volatile& right) {
		left = right;
	}
};

const unsigned int HASH_INT::_EMPTY_HASH = 0;
const unsigned int HASH_INT::_BUSY_HASH  = 1;
const int HASH_INT::_EMPTY_KEY  = 0;
const int HASH_INT::_EMPTY_DATA = 0;
const int HASH_INT::_ADD_SUCCESS = 2; 
const int HASH_INT::_SIZE_INCREASED = 3;

////////////////////////////////////////////////////////////////////////////////
// CLASS: ConcurrentHopscotchHashMap
////////////////////////////////////////////////////////////////////////////////
#define  _NULL_DELTA SHRT_MIN 

template <typename	_tKey, 
          typename	_tData,
			 typename	_tHash,
          typename	_tLock,
			 typename	_tMemory>
class HopscotchHashMap {
private:

	// Inner Classes ............................................................
	struct Bucket {
		short				volatile _first_delta;
		short				volatile _next_delta;
		unsigned int	volatile _hash;
		_tKey				volatile _key;
		_tData			volatile _data;

		void init() {
			_first_delta	= _NULL_DELTA;
			_next_delta		= _NULL_DELTA;
			_hash				= _tHash::_EMPTY_HASH;
			_key				= _tHash::_EMPTY_KEY;
			_data				= _tHash::_EMPTY_DATA;
		}
	};

	struct Segment {
		_u32 volatile	_timestamp;
		_tLock	      _lock;

		void init() {
			_timestamp = 0;
			_lock.init();
		}
	};

    inline_ int first_msb_bit_indx(_u32 x) {
        if(0==x) 
            return -1;
        return  __builtin_clz(x)-1;
    }

    //Function that hashes key value
	inline_ unsigned int Calc(unsigned long long key) {
        unsigned long long hashed_value = hash0(key);
        long long index = hashed_value >> (64-i_bits);
        return index;
	}

	// Fields ...................................................................
	_u32 volatile		_segmentShift;
	_u32 volatile		_segmentMask;
	_u32 volatile		_bucketMask;
	Segment*	volatile	_segments;
	Bucket* volatile	_table;

	const int			_cache_mask;
	const bool			_is_cacheline_alignment;

    long (*hash0)(_tKey);
    int i_bits;

	// Constants ................................................................
	static const _u32 _INSERT_RANGE  = 1024*4;
	//static const _u32 _NUM_SEGMENTS	= 1024*1;
	//static const _u32 _SEGMENTS_MASK = _NUM_SEGMENTS-1;
	static const _u32 _RESIZE_FACTOR = 2;

	// Small Utilities ..........................................................
	Bucket* get_start_cacheline_bucket(Bucket* const bucket) {
		return (bucket - ((bucket - _table) & _cache_mask)); //can optimize 
	}

	void remove_key(Segment&			  segment,
                   Bucket* const		  from_bucket,
						 Bucket* const		  key_bucket, 
						 Bucket* const		  prev_key_bucket, 
						 const unsigned int hash) 
	{
		key_bucket->_hash  = _tHash::_EMPTY_HASH;
		key_bucket->_key   = _tHash::_EMPTY_KEY;
		key_bucket->_data  = _tHash::_EMPTY_DATA;

		if(NULL == prev_key_bucket) {
			if (_NULL_DELTA == key_bucket->_next_delta)
				from_bucket->_first_delta = _NULL_DELTA;
			else 
				from_bucket->_first_delta = (from_bucket->_first_delta + key_bucket->_next_delta);
		} else {
			if (_NULL_DELTA == key_bucket->_next_delta)
				prev_key_bucket->_next_delta = _NULL_DELTA;
			else 
				prev_key_bucket->_next_delta = (prev_key_bucket->_next_delta + key_bucket->_next_delta);
		}

		++(segment._timestamp);
		key_bucket->_next_delta = _NULL_DELTA;
	}
	void add_key_to_begining_of_list(Bucket*	const     keys_bucket, 
										      Bucket*	const		 free_bucket,
												const unsigned int hash,
                                    const _tKey&		 key, 
                                    const _tData& 		 data) 
	{
		free_bucket->_data = data;
		free_bucket->_key  = key;
		free_bucket->_hash = hash;

		if(0 == keys_bucket->_first_delta) {
			if(_NULL_DELTA == keys_bucket->_next_delta)
				free_bucket->_next_delta = _NULL_DELTA;
			else
				free_bucket->_next_delta = (short)((keys_bucket +  keys_bucket->_next_delta) -  free_bucket);
			keys_bucket->_next_delta = (short)(free_bucket - keys_bucket);
		} else {
			if(_NULL_DELTA ==  keys_bucket->_first_delta)
				free_bucket->_next_delta = _NULL_DELTA;
			else
				free_bucket->_next_delta = (short)((keys_bucket +  keys_bucket->_first_delta) -  free_bucket);
			keys_bucket->_first_delta = (short)(free_bucket - keys_bucket);
		}
	}

	void add_key_to_end_of_list(Bucket* const      keys_bucket, 
                               Bucket* const		  free_bucket,
                               const unsigned int hash,
                               const _tKey&		  key, 
										 const _tData&		  data,
                               Bucket* const		  last_bucket)
	{
		free_bucket->_data		 = data;
		free_bucket->_key			 = key;
		free_bucket->_hash		 = hash;
		free_bucket->_next_delta = _NULL_DELTA;

		if(NULL == last_bucket)
			keys_bucket->_first_delta = (short)(free_bucket - keys_bucket);
		else 
			last_bucket->_next_delta = (short)(free_bucket - last_bucket);
	}

	void optimize_cacheline_use(Segment& segment, Bucket* const free_bucket) {
		Bucket* const start_cacheline_bucket(get_start_cacheline_bucket(free_bucket));
		Bucket* const end_cacheline_bucket(start_cacheline_bucket + _cache_mask);
		Bucket* opt_bucket(start_cacheline_bucket);

		do {
			if( _NULL_DELTA != opt_bucket->_first_delta) {
				Bucket* relocate_key_last (NULL);
				int curr_delta(opt_bucket->_first_delta);
				Bucket* relocate_key ( opt_bucket + curr_delta);
				do {
					if( curr_delta < 0 || curr_delta > _cache_mask ) {
						_tHash::relocate_data_reference(free_bucket->_data, relocate_key->_data);
						_tHash::relocate_key_reference(free_bucket->_key, relocate_key->_key);
						free_bucket->_hash  = relocate_key->_hash;

						if(_NULL_DELTA == relocate_key->_next_delta)
							free_bucket->_next_delta = _NULL_DELTA;
						else
							free_bucket->_next_delta = (short)( (relocate_key + relocate_key->_next_delta) - free_bucket );

						if(NULL == relocate_key_last)
							opt_bucket->_first_delta = (short)( free_bucket - opt_bucket );
						else
							relocate_key_last->_next_delta = (short)( free_bucket - relocate_key_last );

						++(segment._timestamp);
						relocate_key->_hash			= _tHash::_EMPTY_HASH;
						_tHash::relocate_key_reference(relocate_key->_key, _tHash::_EMPTY_KEY);
						_tHash::relocate_data_reference(relocate_key->_data, _tHash::_EMPTY_DATA);
						relocate_key->_next_delta	= _NULL_DELTA;
						return;
					}

					if(_NULL_DELTA == relocate_key->_next_delta)
						break;
					relocate_key_last = relocate_key;
					curr_delta += relocate_key->_next_delta;
					relocate_key += relocate_key->_next_delta;
				} while(true);//for on list
			}
			++opt_bucket;
		} while (opt_bucket <= end_cacheline_bucket);
	}

public:// Ctors ................................................................
	HopscotchHashMap(
				_u32 inCapacity				= 32*1024,	//init capacity
				_u32 concurrencyLevel	   = 16,			//num of updating threads
				_u32 cache_line_size       = 64,			//Cache-line size of machine
				bool is_optimize_cacheline = true,
                long (*hash0_arg)(_tKey)   = nullptr)		
	:	_cache_mask					( (cache_line_size / sizeof(Bucket)) - 1 ),
		_is_cacheline_alignment	( is_optimize_cacheline ),
		_segmentMask  ( NearestPowerOfTwo(concurrencyLevel) - 1),
		_segmentShift ( CalcDivideShift(NearestPowerOfTwo(concurrencyLevel/(NearestPowerOfTwo(concurrencyLevel)))-1) )
	{
		//ADJUST INPUT ............................
		const _u32 adjInitCap = NearestPowerOfTwo(inCapacity);
		const _u32 adjConcurrencyLevel = NearestPowerOfTwo(concurrencyLevel);
		const _u32 num_buckets( adjInitCap + _INSERT_RANGE + 1);
		_bucketMask = adjInitCap - 1;
		_segmentShift = first_msb_bit_indx(_bucketMask) - first_msb_bit_indx(_segmentMask);
        
        i_bits = log2l(num_buckets);
        hash0 = hash0_arg;

		//ALLOCATE THE SEGMENTS ...................
		_segments = (Segment*) _tMemory::byte_aligned_malloc( (_segmentMask + 1) * sizeof(Segment) );
		_table = (Bucket*) _tMemory::byte_aligned_malloc( num_buckets * sizeof(Bucket) );

		Segment* curr_seg = _segments;
		for (_u32 iSeg = 0; iSeg <= _segmentMask; ++iSeg, ++curr_seg) {
			curr_seg->init();
		}

		Bucket* curr_bucket = _table;
		for (_u32 iElm=0; iElm < num_buckets; ++iElm, ++curr_bucket) {
			curr_bucket->init();
		}
	}

	~HopscotchHashMap() {
		_tMemory::byte_aligned_free(_table);
		_tMemory::byte_aligned_free(_segments);
	} 

	// Query Operations .........................................................
	inline_ bool containsKey( const _tKey& key ) {

		//CALCULATE HASH ..........................
		const unsigned int hash( Calc(key) );

		//CHECK IF ALREADY CONTAIN ................
		const	Segment&	segment(_segments[(hash >> _segmentShift) & _segmentMask]);

     //go over the list and look for key
		unsigned int start_timestamp;
      do {
			start_timestamp = segment._timestamp;
			const Bucket* curr_bucket( &(_table[hash & _bucketMask]) );
			short next_delta( curr_bucket->_first_delta );
         while( _NULL_DELTA != next_delta ) {
				curr_bucket += next_delta;
				if(hash == curr_bucket->_hash && _tHash::IsEqual(key, curr_bucket->_key))
					return true;
				next_delta = curr_bucket->_next_delta;
			}
		} while(start_timestamp != segment._timestamp);

		return false;
	}

	//modification Operations ...................................................
    std::vector<_tData> range_query(const _tKey& start, const _tKey& end){
        std::vector<_tData> values;

        //Hash Value of start
        const unsigned int hashStart( Calc(start) );
        long segmentStartIdx = (hashStart >> _segmentShift) & _segmentMask;

        //Hash value of end
        const unsigned int hashEnd( Calc(end) );
        long segmentEndIdx = (hashEnd >> _segmentShift) & _segmentMask;

        //lock all the segments needed for range query
        for(auto i = segmentStartIdx; i < segmentEndIdx; i++){
            _segments[i]._lock.lock();
        }

        //for each segment in the range query
        for(auto i = segmentStartIdx; i < segmentEndIdx; i++){
            Segment&	segment(_segments[i]);

            //go over the list and add data to vector if key <= end key
            unsigned int start_timestamp;
            do {
                start_timestamp = segment._timestamp;
                const Bucket* curr_bucket( &(_table[hash & _bucketMask]) );
                short next_delta( curr_bucket->_first_delta );
            while( _NULL_DELTA != next_delta ) {
                    curr_bucket += next_delta;
                    if(curr_bucket->key < end || (hash == curr_bucket->_hash && _tHash::IsEqual(key, curr_bucket->_key)))
                        values.push_back(curr_bucket->_data);
                    next_delta = curr_bucket->_next_delta;
                }
            } while(start_timestamp != segment._timestamp);

        }

        //unlock all the segments needed for range query
        for(auto i = segmentStartIdx; i < segmentEndIdx; i++){
            _segments[i]._lock.unlock();
        }

        return values;
    }
    //Changed this method to return a pair so that we can use it with our driver file
    //TODO: confirm what needs to be returned for sizeIncrease bool
	inline_ std::pair<bool,bool> putIfAbsent(const _tKey& key, const _tData& data) {
		const unsigned int hash( Calc(key) );
		Segment&	segment(_segments[(hash >> _segmentShift) & _segmentMask]);

		//go over the list and look for key
		segment._lock.lock();
		Bucket* const start_bucket( &(_table[hash & _bucketMask]) );

		Bucket* last_bucket( NULL );
		Bucket* compare_bucket( start_bucket );
		short next_delta( compare_bucket->_first_delta );
		while (_NULL_DELTA != next_delta) {
			compare_bucket += next_delta;
			if( hash == compare_bucket->_hash && _tHash::IsEqual(key, compare_bucket->_key) ) {
                //already existed
				// const _tData rc((_tData&)(compare_bucket->_data));
				segment._lock.unlock();
				return {false, false};
			}
			last_bucket = compare_bucket;
			next_delta = compare_bucket->_next_delta;
		}

		//try to place the key in the same cache-line
		if(_is_cacheline_alignment) {
			Bucket*	free_bucket( start_bucket );
			Bucket*	start_cacheline_bucket(get_start_cacheline_bucket(start_bucket));
			Bucket*	end_cacheline_bucket(start_cacheline_bucket + _cache_mask);
			do {
				if( _tHash::_EMPTY_HASH == free_bucket->_hash ) {
					add_key_to_begining_of_list(start_bucket, free_bucket, hash, key, data);
					segment._lock.unlock();
					return {true, false};
				}
				++free_bucket;
				if(free_bucket > end_cacheline_bucket)
					free_bucket = start_cacheline_bucket;
			} while(start_bucket != free_bucket);
		}

		//place key in arbitrary free forward bucket
		Bucket* max_bucket( start_bucket + (SHRT_MAX-1) );
		Bucket* last_table_bucket(_table + _bucketMask);
		if(max_bucket > last_table_bucket)
			max_bucket = last_table_bucket;
		Bucket* free_max_bucket( start_bucket + (_cache_mask + 1) );
		while (free_max_bucket <= max_bucket) {
			if( _tHash::_EMPTY_HASH == free_max_bucket->_hash ) {
				add_key_to_end_of_list(start_bucket, free_max_bucket, hash, key, data, last_bucket);
				segment._lock.unlock();
				return {true, false};
			}
			++free_max_bucket;
		}

		//place key in arbitrary free backward bucket
		Bucket* min_bucket( start_bucket - (SHRT_MAX-1) );
		if(min_bucket < _table)
			min_bucket = _table;
		Bucket* free_min_bucket( start_bucket - (_cache_mask + 1) );
		while (free_min_bucket >= min_bucket) {
			if( _tHash::_EMPTY_HASH == free_min_bucket->_hash ) {
				add_key_to_end_of_list(start_bucket, free_min_bucket, hash, key, data, last_bucket);
				segment._lock.unlock();
				return {true, false};
			}
			--free_min_bucket;
		}

		//NEED TO RESIZE ..........................
		// fprintf(stderr, "ERROR - RESIZE is not implemented - size %u\n", size());
		// exit(1);
        segment._lock.unlock();
        HopscotchWrapper::resize(); 
		return putIfAbsent(key, data);
	}

	inline_ _tData remove(const _tKey& key) {
		//CALCULATE HASH ..........................
		const unsigned int hash(Calc(key));

		//CHECK IF ALREADY CONTAIN ................
		Segment&	segment(_segments[(hash >> _segmentShift) & _segmentMask]);
		segment._lock.lock();
		Bucket* const start_bucket( &(_table[hash & _bucketMask]) );
		Bucket* last_bucket( NULL );
		Bucket* curr_bucket( start_bucket );
		short	  next_delta (curr_bucket->_first_delta);
		do {
			if(_NULL_DELTA == next_delta) {
				segment._lock.unlock();
				return _tHash::_EMPTY_DATA;
			}
			curr_bucket += next_delta;

			if( hash == curr_bucket->_hash && _tHash::IsEqual(key, curr_bucket->_key) ) {
				_tData const rc((_tData&)(curr_bucket->_data));
				remove_key(segment, start_bucket, curr_bucket, last_bucket, hash);
				if( _is_cacheline_alignment )
					optimize_cacheline_use(segment, curr_bucket);
				segment._lock.unlock();
				return rc;
			}
			last_bucket = curr_bucket;
			next_delta = curr_bucket->_next_delta;
		} while(true);
		return _tHash::_EMPTY_DATA;
	}

	//status Operations .........................................................
	unsigned int size() {
		_u32 counter = 0;
		const _u32 num_elm( _bucketMask + _INSERT_RANGE );
		for(_u32 iElm=0; iElm < num_elm; ++iElm) {
			if( _tHash::_EMPTY_HASH != _table[iElm]._hash ) {
				++counter;
			}
		}
		return counter;
	}   

	double percentKeysInCacheline() {
		unsigned int total_in_cache( 0 );
		unsigned int total( 0 );

		Bucket* curr_bucket(_table);
		for(int iElm(0); iElm <= _bucketMask; ++iElm, ++curr_bucket) {

			if(_NULL_DELTA != curr_bucket->_first_delta) {
				Bucket* const startCacheLineBucket( get_start_cacheline_bucket(curr_bucket) );
				Bucket* check_bucket(curr_bucket + curr_bucket->_first_delta);
				int currDist( curr_bucket->_first_delta );
				do {
					++total;
					if( (check_bucket - startCacheLineBucket)  >= 0 && (check_bucket - startCacheLineBucket) <= _cache_mask )
						++total_in_cache;
					if(_NULL_DELTA == check_bucket->_next_delta)
						break;
					currDist += check_bucket->_next_delta;
					check_bucket += check_bucket->_next_delta;
				} while(true);
			}
		}

		//return percent in cache
		return (((double)total_in_cache)/((double)total)*100.0);
	}

private:
	// Private Static Utilities .................................................
	static _u32 NearestPowerOfTwo(const _u32 value)	{
		_u32 rc( 1 );
		while (rc < value) {
			rc <<= 1;
		}
		return rc;
	}

	static unsigned int CalcDivideShift(const unsigned int _value) {
		unsigned int numShift( 0 );
		unsigned int curr( 1 );
		while (curr < _value) {
			curr <<= 1;
			++numShift;
		}
		return numShift;
	}

};

#endif

//};




