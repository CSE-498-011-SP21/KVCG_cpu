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

#define  _NULL_DELTA SHRT_MIN 

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

// #define MB()  __asm__ __volatile__ (";; mf ;; " : : : "memory")
// #define WMB() MB()
// #define RMB() MB()
#define RMB() _GLIBCXX_READ_MEM_BARRIER
#define WMB() _GLIBCXX_WRITE_MEM_BARRIER
#define MB()  __sync_synchronize()
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

template<typename V>
	class VolatileType {
		V volatile _value;
	public:
		inline_ explicit VolatileType() { }
		inline_ explicit VolatileType(const V& x) {
			_value = x;
			Memory::write_barrier();
		}
		inline_ V get() const {
			Memory::read_barrier();
			return _value;
		}

		inline_ V getNotSafe() const {
			return _value;
		}

		inline_ void set(const V& x) {
			_value = x;
			Memory::write_barrier();
		}
		inline_ void setNotSafe(const V& x) {
			_value = x;
		}

		//--------------
		inline_ operator V() const {  //convention
			return get();
		}
		inline_ V operator->() const {
			return get();
		}
		inline_ V volatile * operator&() {
			Memory::read_barrier();
			return &_value;
		}

		//--------------
		inline_ VolatileType<V>& operator=(const V x) { 
			_value = x;
			Memory::write_barrier();
			return *this;
		}
		//--------------
		inline_ bool operator== (const V& right) const {
			Memory::read_barrier();
			return _value == right;
		}
		inline_ bool operator!= (const V& right) const {
			Memory::read_barrier();
			return _value != right;
		}
		inline_ bool operator== (const VolatileType<V>& right) const {
			Memory::read_barrier();
			return _value == right._value;
		}
		inline_ bool operator!= (const VolatileType<V>& right) const {
			Memory::read_barrier();
			return _value != right._value;
		}

		//--------------
		inline_ VolatileType<V>& operator++ () { //prefix ++
			++_value;
			Memory::write_barrier();
			return *this;
		}
		inline_ V operator++ (int) { //postfix ++
			const V tmp(_value);
			++_value;
			Memory::write_barrier();
			return tmp;
		}
		inline_ VolatileType<V>& operator-- () {// prefix --
			--_value;
			Memory::write_barrier();
			return *this;
		}
		inline_ V operator-- (int) {// postfix --
			const V tmp(_value);
			--_value;
			Memory::write_barrier();
			return tmp;
		}

		//--------------
		inline_ VolatileType<V>& operator+=(const V& x) { 
			_value += x;
			Memory::write_barrier();
			return *this;
		}
		inline_ VolatileType<V>& operator-=(const V& x) { 
			_value -= x;
			Memory::write_barrier();
			return *this;
		}
		inline_ VolatileType<V>& operator*=(const V& x) { 
			_value *= x;
			Memory::write_barrier();
			return *this;
		}
		inline_ VolatileType<V>& operator/=(const V& x) { 
			_value /= x;
			Memory::write_barrier();
			return *this;
		}
		//--------------
		inline_ V operator+(const V& x) { 
			Memory::read_barrier();
			return _value + x;
		}
		inline_ V operator-(const V& x) { 
			Memory::read_barrier();
			return _value - x;
		}
		inline_ V operator*(const V& x) { 
			Memory::read_barrier();
			return _value * x;
		}
		inline_ V operator/(const V& x) { 
			Memory::read_barrier();
			return _value / x;
		}
	};

inline unsigned MUTEX_ENTER(unsigned volatile* x) {
	return __sync_lock_test_and_set(x, 0xFF);
}

inline void MUTEX_EXIT(unsigned volatile* x) {
	return __sync_lock_release (x);
}

class TTASLock {
public:
    VolatileType<_u32> _lock;

    TTASLock() : _lock(0) {}
    ~TTASLock() {_lock=0;}

    inline_ void init() {_lock = 0;}

    inline_ void lock() {
        do {
            if(0 == _lock && (0 == MUTEX_ENTER(&_lock))) {
                return;
            }
        } while(true);
    }

    inline_ bool tryLock() {
        return ( 0 == _lock && 0 == MUTEX_ENTER(&_lock) );
    }

    inline_ bool isLocked() {
        return 0 != _lock;
    }

    inline_ void unlock() {
        MUTEX_EXIT(&_lock);
    }
};

const unsigned int HASH_INT::_EMPTY_HASH = 0;
const unsigned int HASH_INT::_BUSY_HASH  = 1;
const int HASH_INT::_EMPTY_KEY  = 0;
const int HASH_INT::_EMPTY_DATA = 0;
const int HASH_INT::_ADD_SUCCESS = 2; 
const int HASH_INT::_SIZE_INCREASED = 3;

//CLASS hopscotch_v3
template <typename K,typename V>
class hopscotch_v3: public cpu_cache<K, V>{

    private: 

        //Private Static Utilities
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

        // Inner Classes ............................................................
        struct Bucket {
            short				volatile _first_delta;
            short				volatile _next_delta;
            unsigned int	volatile _hash;
            K				volatile _key;
            V			volatile _data;

            void init() {
                _first_delta	= _NULL_DELTA;
                _next_delta		= _NULL_DELTA;
                _hash				= HASH_INT::_EMPTY_HASH;
                _key				= HASH_INT::_EMPTY_KEY;
                _data				= HASH_INT::_EMPTY_DATA;
            }
        };

        struct Segment {
            _u32 volatile	_timestamp;
            TTASLock	      _lock;

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

        int			_cache_mask;
        bool			_is_cacheline_alignment;

        long (*hash0)(K);
        K (*get_random_K)();
        V (*get_random_V)();
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
            key_bucket->_hash  = HASH_INT::_EMPTY_HASH;
            key_bucket->_key   = HASH_INT::_EMPTY_KEY;
            key_bucket->_data  = HASH_INT::_EMPTY_DATA;

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
                                        const K&		 key, 
                                        const V& 		 data) 
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
                                const K&		  key, 
                                            const V&		  data,
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
                            HASH_INT::relocate_data_reference(free_bucket->_data, relocate_key->_data);
                            HASH_INT::relocate_key_reference(free_bucket->_key, relocate_key->_key);
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
                            relocate_key->_hash			= HASH_INT::_EMPTY_HASH;
                            HASH_INT::relocate_key_reference(relocate_key->_key, HASH_INT::_EMPTY_KEY);
                            HASH_INT::relocate_data_reference(relocate_key->_data, HASH_INT::_EMPTY_DATA);
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
        
  
    public:

        hopscotch_v3(_u32 inCapacity,	//init capacity
                    _u32 concurrencyLevel,			//num of updating threads
                    _u32 cache_line_size,			//Cache-line size of machine
                    bool is_optimize_cacheline, 
                    K (*get_random_K_arg)(), V (*get_random_V_arg)(), long (*hash0_arg)(K)){
                
            _cache_mask = (cache_line_size / sizeof(Bucket)) - 1;
            _is_cacheline_alignment = is_optimize_cacheline;
            _segmentMask = NearestPowerOfTwo(concurrencyLevel) - 1;
            _segmentShift = CalcDivideShift(NearestPowerOfTwo(concurrencyLevel/(NearestPowerOfTwo(concurrencyLevel)))-1);

            //ADJUST INPUT ............................
            const _u32 adjInitCap = NearestPowerOfTwo(inCapacity);
            //const _u32 adjConcurrencyLevel = NearestPowerOfTwo(concurrencyLevel);
            const _u32 num_buckets( adjInitCap + _INSERT_RANGE + 1);
            _bucketMask = adjInitCap - 1;
            _segmentShift = first_msb_bit_indx(_bucketMask) - first_msb_bit_indx(_segmentMask);
            
            i_bits = log2l(num_buckets);
            hash0 = hash0_arg;
            get_random_K = get_random_K_arg;
            get_random_V = get_random_V_arg;

            //ALLOCATE THE SEGMENTS ...................
            _segments = (Segment*) Memory::byte_aligned_malloc( (_segmentMask + 1) * sizeof(Segment) );
            _table = (Bucket*) Memory::byte_aligned_malloc( num_buckets * sizeof(Bucket) );

            Segment* curr_seg = _segments;
            for (_u32 iSeg = 0; iSeg <= _segmentMask; ++iSeg, ++curr_seg) {
                curr_seg->init();
            }

            Bucket* curr_bucket = _table;
            for (_u32 iElm=0; iElm < num_buckets; ++iElm, ++curr_bucket) {
                curr_bucket->init();
            }
        }

        ~hopscotch_v3(){
            Memory::byte_aligned_free(_table);
            Memory::byte_aligned_free(_segments);
        }


        std::pair<bool, bool> add(K key_to_add, V val_to_add){
            std::cout << "Time to add!\n" << std::endl;
            const unsigned int hash( Calc(key_to_add) );
            Segment&	segment(_segments[(hash >> _segmentShift) & _segmentMask]);

            //go over the list and look for key
            segment._lock.lock();
            Bucket* const start_bucket( &(_table[hash & _bucketMask]) );

            Bucket* last_bucket( NULL );
            Bucket* compare_bucket( start_bucket );
            short next_delta( compare_bucket->_first_delta );
            while (_NULL_DELTA != next_delta) {
                compare_bucket += next_delta;
                if( hash == compare_bucket->_hash && HASH_INT::IsEqual(key_to_add, compare_bucket->_key) ) {
                    //already existed
                    // const V rc((V&)(compare_bucket->_data));
                    std::cout << "1\n" << std::endl;
                    segment._lock.unlock();
                    return {false, false};
                }
                last_bucket = compare_bucket;
                next_delta = compare_bucket->_next_delta;
            }
            std::cout << "now\n" << std::endl;
            //try to place the key in the same cache-line
            if(_is_cacheline_alignment) {
                Bucket*	free_bucket( start_bucket );
                Bucket*	start_cacheline_bucket(get_start_cacheline_bucket(start_bucket));
                Bucket*	end_cacheline_bucket(start_cacheline_bucket + _cache_mask);
                do {
                    if( HASH_INT::_EMPTY_HASH == free_bucket->_hash ) {
                        std::cout << "2\n" << std::endl;
                        add_key_to_begining_of_list(start_bucket, free_bucket, hash, key_to_add, val_to_add);
                        segment._lock.unlock();
                        return {true, false};
                    }
                    ++free_bucket;
                    if(free_bucket > end_cacheline_bucket)
                        free_bucket = start_cacheline_bucket;
                } while(start_bucket != free_bucket);
            }
            std::cout << "here\n" << std::endl;
            //place key in arbitrary free forward bucket
            Bucket* max_bucket( start_bucket + (SHRT_MAX-1) );
            Bucket* last_table_bucket(_table + _bucketMask);
            if(max_bucket > last_table_bucket)
                max_bucket = last_table_bucket;
            Bucket* free_max_bucket( start_bucket + (_cache_mask + 1) );
            while (free_max_bucket <= max_bucket) {
                if( HASH_INT::_EMPTY_HASH == free_max_bucket->_hash ) {
                    std::cout << "3\n" << std::endl;
                    add_key_to_end_of_list(start_bucket, free_max_bucket, hash, key_to_add, val_to_add, last_bucket);
                    segment._lock.unlock();
                    return {true, false};
                }
                ++free_max_bucket;
            }
            std::cout << "heyo\n" << std::endl;
            //place key in arbitrary free backward bucket
            Bucket* min_bucket( start_bucket - (SHRT_MAX-1) );
            if(min_bucket < _table)
                min_bucket = _table;
            Bucket* free_min_bucket( start_bucket - (_cache_mask + 1) );
            while (free_min_bucket >= min_bucket) {
                if( HASH_INT::_EMPTY_HASH == free_min_bucket->_hash ) {
                    std::cout << "4\n" << std::endl;
                    add_key_to_end_of_list(start_bucket, free_min_bucket, hash, key_to_add, val_to_add, last_bucket);
                    segment._lock.unlock();
                    return {true, false};
                }
                --free_min_bucket;
            }
            std::cout << "heehehe\n" << std::endl;
            segment._lock.unlock();
            // resize(); 
            // return add(key_to_add, val_to_add);
            std::cout << "5\n" << std::endl;
            //NEED TO RESIZE ..........................
            fprintf(stderr, "ERROR - RESIZE is not implemented - size %ld\n", size());
            // exit(1);
            return {false, false};
        }
    
        bool remove(K to_remove){
            int res;
            //CALCULATE HASH ..........................
            const unsigned int hash(Calc(to_remove));

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
                    res = HASH_INT::_EMPTY_DATA;
                }
                curr_bucket += next_delta;

                if( hash == curr_bucket->_hash && HASH_INT::IsEqual(to_remove, curr_bucket->_key) ) {
                    V const rc((V&)(curr_bucket->_data));
                    remove_key(segment, start_bucket, curr_bucket, last_bucket, hash);
                    if( _is_cacheline_alignment )
                        optimize_cacheline_use(segment, curr_bucket);
                    segment._lock.unlock();
                    res = rc;
                }
                last_bucket = curr_bucket;
                next_delta = curr_bucket->_next_delta;
            } while(true);
            res = HASH_INT::_EMPTY_DATA;

            if (res == HASH_INT::_EMPTY_DATA){
                return false;
            }
            return true;
        }

        bool contains(K to_find){
        //CALCULATE HASH ..........................
            const unsigned int hash( Calc(to_find) );

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
                    if(hash == curr_bucket->_hash && HASH_INT::IsEqual(to_find, curr_bucket->_key))
                        return true;
                    next_delta = curr_bucket->_next_delta;
                }
            } while(start_timestamp != segment._timestamp);

            return false;
        }

        std::vector<V> range_query(K start, K end){
            std::vector<V> values;
            if(start > end){
                return values;
            }

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
            
            //starting index of bucket in _table
            auto j =  hashStart & _bucketMask;       

            //for each segment in the range query
            for(auto i = segmentStartIdx; i < segmentEndIdx; i++){
                Segment&	segment(_segments[i]);

                //go over the list and add data to vector if key <= end key
                unsigned int start_timestamp;
                do {
                    start_timestamp = segment._timestamp;
                    const Bucket* curr_bucket( &(_table[j]) );
                    short next_delta( curr_bucket->_first_delta );
                while( _NULL_DELTA != next_delta ) {
                        curr_bucket += next_delta;
                        if(curr_bucket->_key <= end){
                            V val = curr_bucket->_data;
                            values.push_back(val);
                        }
                        next_delta = curr_bucket->_next_delta;
                    }
                    j++;
                } while(start_timestamp != segment._timestamp);
            }

            //unlock all the segments needed for range query
            for(auto i = segmentStartIdx; i < segmentEndIdx; i++){
                _segments[i]._lock.unlock();
            }

            return values;
        }

        long size(){
            long counter = 0;
            const long num_elm( _bucketMask + _INSERT_RANGE );
            for(_u32 iElm=0; iElm < num_elm; ++iElm) {
                if( HASH_INT::_EMPTY_HASH != _table[iElm]._hash ) {
                    ++counter;
                }
            }
            return counter;
        }

        void populate(long num_elements){
            for(long i=0; i<num_elements; i++){
                // Ensures effective adds
                while(!add(get_random_K(), get_random_V()).second);
            }
        }

        // void resize(){
        //     this->hopscotch_v3::~hopscotch_v3();
        //     this->hopscotch_v3::hopscotch_v3(&get_random_int, &get_random_int, &hash_int_0, 32*1024*2, 16, 64, true);
        //     return;
        //     //TODO: ask if this will work, will the return to the add method not use this new ds?
        // }
    
};