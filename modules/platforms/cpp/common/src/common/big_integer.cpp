/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "ignite/ignite_error.h"
#include "ignite/common/bits.h"
#include "ignite/common/big_integer.h"

namespace ignite
{
    namespace common
    {
        BigInteger::BigInteger() :
            sign(1),
            mag()
        {
            // No-op.
        }

        BigInteger::BigInteger(int64_t val) :
            sign(1),
            mag()
        {
            AssignInt64(val);
        }

        BigInteger::BigInteger(const char* val, int32_t len) :
            sign(1),
            mag()
        {
            AssignString(val, len);
        }

        BigInteger::BigInteger(const BigInteger& other) :
            sign(other.sign),
            mag(other.mag)
        {
            // No-op.
        }

        BigInteger::BigInteger(const int8_t* val, int32_t len, int32_t sign, bool bigEndian) :
            sign(sign),
            mag()
        {
            assert(val != 0);
            assert(len >= 0);
            assert(sign == 1 || sign == 0 || sign == -1);

            if (bigEndian)
            {
                int32_t firstNonZero = 0;
                while (firstNonZero < len && val[firstNonZero] == 0)
                    ++firstNonZero;

                if (firstNonZero == len)
                {
                    AssignInt64(0);

                    return;
                }

                int32_t intLength = (len - firstNonZero + 3) / 4;

                mag.Resize(intLength);

                int32_t b = len - 1;

                for (int32_t i = 0; i < intLength - 1; ++i)
                {
                    mag[i] = (val[b] & 0xFFUL)
                        | ((val[b - 1] & 0xFFUL) << 8)
                        | ((val[b - 2] & 0xFFUL) << 16)
                        | ((val[b - 3] & 0xFFUL) << 24);

                    b -= 4;
                }

                int32_t bytesRemaining = b - firstNonZero + 1;

                assert(bytesRemaining > 0 && bytesRemaining <= 4);

                switch (bytesRemaining)
                {
                    case 4:
                        mag[intLength - 1] |= (val[b - 3] & 0xFF) << 24;
                        // Fall-through.

                    case 3:
                        mag[intLength - 1] |= (val[b - 2] & 0xFF) << 16;
                        // Fall-through.

                    case 2:
                        mag[intLength - 1] |= (val[b - 1] & 0xFF) << 8;
                        // Fall-through.

                    case 1:
                        mag[intLength - 1] |= val[b] & 0xFF;
                        // Fall-through.

                    default:
                        break;
                }
            }
            else
            {
                int32_t firstNonZero = len - 1;

                while (firstNonZero >= 0 && val[firstNonZero] == 0)
                    --firstNonZero;

                if (firstNonZero == -1)
                {
                    AssignInt64(0);

                    return;
                }

                int32_t intLength = (firstNonZero + 4) / 4;

                mag.Resize(intLength);

                int32_t b = 0;

                for (int32_t i = 0; i < intLength - 1; ++i)
                {
                    mag[i] = (val[b] & 0xFFUL)
                        | ((val[b + 1] & 0xFFUL) << 8)
                        | ((val[b + 2] & 0xFFUL) << 16)
                        | ((val[b + 3] & 0xFFUL) << 24);

                    b += 4;
                }

                int32_t bytesRemaining = firstNonZero - b + 1;

                assert(bytesRemaining > 0 && bytesRemaining <= 4);

                switch (bytesRemaining)
                {
                    case 4:
                        mag[intLength - 1] |= (val[b + 3] & 0xFF) << 24;
                        // Fall-through.

                    case 3:
                        mag[intLength - 1] |= (val[b + 2] & 0xFF) << 16;
                        // Fall-through.

                    case 2:
                        mag[intLength - 1] |= (val[b + 1] & 0xFF) << 8;
                        // Fall-through.

                    case 1:
                        mag[intLength - 1] |= val[b] & 0xFF;
                        // Fall-through.

                    default:
                        break;
                }
            }
        }

        BigInteger::BigInteger(MagArray &mag, int8_t sign) :
            sign(sign),
            mag()
        {
            this->mag.Swap(mag);
        }

        BigInteger& BigInteger::operator=(const BigInteger& other)
        {
            Assign(other);

            return *this;
        }

        void BigInteger::Assign(const BigInteger& val)
        {
            if (this != &val)
            {
                sign = val.sign;
                mag = val.mag;
            }
        }

        void BigInteger::AssignInt64(int64_t val)
        {
            if (val < 0)
            {
                AssignUint64(static_cast<uint64_t>(-val));

                sign = -1;
            }
            else
                AssignUint64(static_cast<uint64_t>(val));
        }

        void BigInteger::AssignString(const char* val, int32_t len)
        {
            std::stringstream converter;

            converter.write(val, len);

            converter >> *this;
        }

        void BigInteger::AssignUint64(uint64_t val)
        {
            sign = 1;

            if (val == 0)
            {
                mag.Clear();

                return;
            }

            uint32_t highWord = static_cast<uint32_t>(val >> 32);

            if (highWord == 0)
                mag.Resize(1);
            else
            {
                mag.Resize(2);
                mag[1] = highWord;
            }

            mag[0] = static_cast<uint32_t>(val);
        }

        int8_t BigInteger::GetSign() const
        {
            return sign;
        }

        void BigInteger::Swap(BigInteger& other)
        {
            using std::swap;

            swap(sign, other.sign);
            mag.Swap(other.mag);
        }

        const BigInteger::MagArray& BigInteger::GetMagnitude() const
        {
            return mag;
        }

        uint32_t BigInteger::GetBitLength() const
        {
            if (mag.IsEmpty())
                return 0;

            int32_t res = bits::BitLengthU32(mag[mag.GetSize() - 1]);

            if (mag.GetSize() > 1)
                res += (mag.GetSize() - 1) * 32;

            return res;
        }

        int32_t BigInteger::GetPrecision() const
        {
            // See http://graphics.stanford.edu/~seander/bithacks.html
            // for the details on the algorithm.

            if (mag.GetSize() == 0)
                return 1;

            int32_t r = static_cast<uint32_t>(((
                static_cast<uint64_t>(GetBitLength()) + 1) * 646456993) >> 31);

            BigInteger prec;
            BigInteger::GetPowerOfTen(r, prec);

            return Compare(prec, true) < 0 ? r : r + 1;
        }

        void BigInteger::MagnitudeToBytes(common::FixedSizeArray<int8_t>& buffer) const
        {
            int32_t bytesNum = static_cast<int32_t>((GetBitLength() + 7) / 8);

            if (bytesNum == 0)
            {
                buffer.Reset(1);

                return;
            }

            buffer.Reset(bytesNum);

            int32_t i;
            for (i = 0; i < mag.GetSize() - 1; ++i)
            {
                int32_t j = bytesNum - 1 - 4 * i;

                buffer[j] = static_cast<int8_t>(mag[i]);
                buffer[j - 1] = static_cast<int8_t>(mag[i] >> 8);
                buffer[j - 2] = static_cast<int8_t>(mag[i] >> 16);
                buffer[j - 3] = static_cast<int8_t>(mag[i] >> 24);
            }

            int32_t bytesRemaining = bytesNum - 4 * i;

            assert(bytesRemaining >= 0 && bytesRemaining <= 4);

            i = 0;
            switch (bytesRemaining)
            {
                case 4:
                    buffer[i++] |= static_cast<int8_t>(mag[mag.GetSize() - 1] >> 24);
                    // Fall-through.

                case 3:
                    buffer[i++] |= static_cast<int8_t>(mag[mag.GetSize() - 1] >> 16);
                    // Fall-through.

                case 2:
                    buffer[i++] |= static_cast<int8_t>(mag[mag.GetSize() - 1] >> 8);
                    // Fall-through.

                case 1:
                    buffer[i++] |= static_cast<int8_t>(mag[mag.GetSize() - 1]);
                    // Fall-through.

                default:
                    break;
            }
        }

        void BigInteger::Pow(int32_t exp)
        {
            if (exp < 0)
            {
                AssignInt64(0);

                return;
            }

            uint32_t bitsLen = GetBitLength();

            if (!bitsLen)
                return;

            if (bitsLen == 1)
            {
                if ((exp % 2 == 0) && sign < 0)
                    sign = -sign;

                return;
            }

            BigInteger multiplicant(*this);
            AssignInt64(1);

            int32_t mutExp = exp;
            while (mutExp)
            {
                if (mutExp & 1)
                    Multiply(multiplicant, *this);

                mutExp >>= 1;

                if (mutExp)
                    multiplicant.Multiply(multiplicant, multiplicant);
            }
        }

        void BigInteger::Multiply(const BigInteger& other, BigInteger& res) const
        {
            MagArray resMag(mag.GetSize() + other.mag.GetSize());

            resMag.Resize(mag.GetSize() + other.mag.GetSize());

            for (int32_t i = 0; i < other.mag.GetSize(); ++i)
            {
                uint32_t carry = 0;

                for (int32_t j = 0; j < mag.GetSize(); ++j)
                {
                    uint64_t product = static_cast<uint64_t>(mag[j]) * other.mag[i] + 
                        + resMag[i + j] + carry;

                    resMag[i + j] = static_cast<uint32_t>(product);
                    carry = static_cast<uint32_t>(product >> 32);
                }

                resMag[i + mag.GetSize()] = carry;
            }

            res.mag.Swap(resMag);
            res.sign = sign * other.sign;

            res.Normalize();
        }

        /**
         * Shift magnitude left by the specified number of bits.
         *
         * @param in Input magnitude.
         * @param len Magnitude length.
         * @param out Output magnitude. Should be not shorter than the input
         *     magnitude.
         * @param n Number of bits to shift to.
         */
        void ShiftLeft(const uint32_t* in, int32_t len, uint32_t* out, unsigned n)
        {
            assert(n < 32);

            if (n == 0)
            {
                std::copy(in, in + len, out);

                return;
            }

            for (int32_t i = len - 1; i > 0; --i)
                out[i] = (in[i] << n) | (in[i - 1] >> (32 - n));

            out[0] = in[0] << n;
        }

        /**
         * Shift magnitude right by the specified number of bits.
         *
         * @param in Input magnitude.
         * @param len Magnitude length.
         * @param out Output magnitude. Should be not shorter than the input
         *     magnitude.
         * @param n Number of bits to shift to.
         */
        void ShiftRight(const uint32_t* in, int32_t len, uint32_t* out, unsigned n)
        {
            assert(n < 32);

            if (n == 0)
            {
                std::copy(in, in + len, out);

                return;
            }

            for (int32_t i = 0; i < len - 1; ++i)
                out[i] = (in[i] >> n) | (in[i + 1] << (32 - n));

            out[len - 1] = in[len - 1] >> n;
        }

        /**
         * Part of the division algorithm. Computes q - (a * x).
         *
         * @param q Minuend.
         * @param a Multipliplier of the subtrahend.
         * @param alen Length of the a.
         * @param x Multipliplicand of the subtrahend.
         * @return Carry.
         */
        uint32_t MultiplyAndSubstruct(uint32_t* q, const uint32_t* a, int32_t alen, uint32_t x)
        {
            uint64_t carry = 0;

            for (int32_t i = 0; i < alen; ++i)
            {
                uint64_t product = a[i] * static_cast<uint64_t>(x);
                int64_t difference = q[i] - carry - (product & 0xFFFFFFFF);

                q[i] = static_cast<uint32_t>(difference);

                // This will add one if difference is negative.
                carry = (product >> 32) - (difference >> 32);
            }

            return static_cast<uint32_t>(carry);
        }

        /**
         * Add two magnitude arrays and return carry.
         *
         * @param res First addend. Result is placed here. Length of this addend
         *     should be equal or greater than len.
         * @param addend Second addend.
         * @param len Length of the second addend.
         * @return Carry.
         */
        uint32_t Add(uint32_t* res, const uint32_t* addend, int32_t len)
        {
            uint64_t carry = 0;

            for (int32_t i = 0; i < len; ++i)
            {
                uint64_t sum = static_cast<uint64_t>(res[i]) + addend[i] + carry;
                res[i] = static_cast<uint32_t>(sum);
                carry = sum >> 32;
            }

            return static_cast<uint32_t>(carry);
        }

        /**
         * Add single number to a magnitude array and return carry.
         *
         * @param res First addend. Result is placed here. Length of this addend
         *     should be equal or greater than len.
         * @param len Length of the First addend.
         * @param addend Second addend.
         * @return Carry.
         */
        uint32_t Add(uint32_t* res, int32_t len, uint32_t addend)
        {
            uint64_t carry = addend;

            for (int32_t i = 0; (i < len) && carry; ++i)
            {
                uint64_t sum = static_cast<uint64_t>(res[i]) + carry;
                res[i] = static_cast<uint32_t>(sum);
                carry = sum >> 32;
            }

            return static_cast<uint32_t>(carry);
        }

        void BigInteger::Divide(const BigInteger& divisor, BigInteger& res) const
        {
            Divide(divisor, res, 0);
        }

        void BigInteger::Divide(const BigInteger& divisor, BigInteger& res, BigInteger& rem) const
        {
            Divide(divisor, res, &rem);
        }

        void BigInteger::Add(const uint32_t* addend, int32_t len)
        {
            if (mag.GetSize() < len)
            {
                mag.Reserve(len + 1);

                mag.Resize(len);
            }
            else
                mag.Reserve(mag.GetSize() + 1);

            uint32_t carry = common::Add(mag.GetData(), addend, len);

            if (carry)
            {
                carry = common::Add(mag.GetData() + len, mag.GetSize() - len, carry);

                if (carry)
                    mag.PushBack(carry);
            }
        }

        void BigInteger::Add(uint64_t x)
        {
            if (x == 0)
                return;

            if (IsZero())
            {
                AssignUint64(x);

                return;
            }

            uint32_t val[2];

            val[0] = static_cast<uint32_t>(x);
            val[1] = static_cast<uint32_t>(x >> 32);

            Add(val, val[1] ? 2 : 1);
        }

        int32_t BigInteger::Compare(const BigInteger& other, bool ignoreSign) const
        {
            // What we should return if magnitude is greater.
            int32_t mgt = 1;

            if (!ignoreSign)
            {
                if (sign != other.sign)
                    return sign > other.sign ? 1 : -1;
                else
                    mgt = sign;
            }

            if (mag.GetSize() != other.mag.GetSize())
                return mag.GetSize() > other.mag.GetSize() ? mgt : -mgt;

            for (int32_t i = mag.GetSize() - 1; i >= 0; --i)
            {
                if (mag[i] == other.mag[i])
                    continue;
                else if (mag[i] > other.mag[i])
                    return mgt;
                else
                    return -mgt;
            }

            return 0;
        }

        int64_t BigInteger::ToInt64() const
        {
            return (static_cast<uint64_t>(GetMagInt(1)) << 32) | GetMagInt(0);
        }

        void BigInteger::GetPowerOfTen(int32_t pow, BigInteger& res)
        {
            using namespace common;

            assert(pow >= 0);

            if (pow < bits::UINT64_MAX_PRECISION)
            {
                res.AssignUint64(bits::TenPowerU64(pow));

                return;
            }

            res.AssignInt64(10);
            res.Pow(pow);
        }

        uint32_t BigInteger::GetMagInt(int32_t n) const
        {
            assert(n >= 0);

            if (n >= mag.GetSize())
                return sign > 0 ? 0 : -1;

            return sign * mag[n];
        }

        void BigInteger::Divide(const BigInteger& divisor, BigInteger& res, BigInteger* rem) const
        {
            // Can't divide by zero.
            if (divisor.mag.IsEmpty())
                throw IgniteError(IgniteError::IGNITE_ERR_ILLEGAL_ARGUMENT, "Division by zero.");

            int32_t compRes = Compare(divisor, true);

            int8_t resSign = sign * divisor.sign;

            // The same magnitude. Result is [-]1 and remainder is zero.
            if (compRes == 0)
            {
                res.AssignInt64(resSign);

                if (rem)
                    rem->AssignInt64(0);

                return;
            }

            // Divisor is greater than this. Result is 0 and remainder is this.
            if (compRes == -1)
            {
                // Order is important here! Copy to rem first to handle the case
                // when &res == this.
                if (rem)
                    rem->Assign(*this);

                res.AssignInt64(0);

                return;
            }

            // If divisor is [-]1 result is [-]this and remainder is zero.
            if (divisor.GetBitLength() == 1)
            {
                // Once again: order is important.
                res.Assign(*this);
                res.sign = sign * divisor.sign;

                if (rem)
                    rem->AssignInt64(0);

                return;
            }

            // Trivial case.
            if (mag.GetSize() <= 2)
            {
                uint64_t u = mag[0];
                uint64_t v = divisor.mag[0];

                if (mag.GetSize() == 2)
                    u |= static_cast<uint64_t>(mag[1]) << 32;

                if (divisor.mag.GetSize() == 2)
                    v |= static_cast<uint64_t>(divisor.mag[1]) << 32;

                // Divisor can not be 1, or 0.
                assert(v > 1);

                // It should also be less than dividend.
                assert(v < u);

                // (u / v) is always fits into int64_t because abs(v) >= 2.
                res.AssignInt64(resSign * static_cast<int64_t>(u / v));

                // (u % v) is always fits into int64_t because (u > v) ->
                // (u % v) < (u / 2).
                if (rem)
                    rem->AssignInt64(resSign * static_cast<int64_t>(u % v));

                return;
            }

            // Using Knuth division algorithm D for common case.

            // Short aliases.
            const MagArray& u = mag;
            const MagArray& v = divisor.mag;
            MagArray& q = res.mag;
            int32_t ulen = u.GetSize();
            int32_t vlen = v.GetSize();

            // First we need to normilize divisor.
            MagArray nv;
            nv.Resize(v.GetSize());

            int32_t shift = bits::NumberOfLeadingZerosU32(v.Back());
            ShiftLeft(v.GetData(), vlen, nv.GetData(), shift);

            // Divisor is normilized. Now we need to normilize divident.
            MagArray nu;

            // First find out what is the size of it.
            if (bits::NumberOfLeadingZerosU32(u.Back()) >= shift)
            {
                // Everything is the same as with divisor. Just add leading zero.
                nu.Resize(ulen + 1);

                ShiftLeft(u.GetData(), ulen, nu.GetData(), shift);

                assert((static_cast<uint64_t>(u.Back()) >> (32 - shift)) == 0);
            }
            else
            {
                // We need one more byte here. Also adding leading zero.
                nu.Resize(ulen + 2);

                ShiftLeft(u.GetData(), ulen, nu.GetData(), shift);

                nu[ulen] = u[ulen - 1] >> (32 - shift);

                assert(nu[ulen] != 0);
            }

            assert(nu.Back() == 0);

            // Resizing resulting array.
            q.Resize(ulen - vlen + 1);

            // Main loop
            for (int32_t i = ulen - vlen; i >= 0; --i)
            {
                uint64_t base = bits::MakeU64(nu[i + vlen], nu[i + vlen - 1]);

                uint64_t qhat = base / nv[vlen - 1]; // Estimated quotient. 
                uint64_t rhat = base % nv[vlen - 1]; // A remainder.

                // Adjusting result if needed.
                while (qhat > UINT32_MAX ||
                      ((qhat * nv[vlen - 2]) > ((UINT32_MAX + static_cast<uint64_t>(1)) * rhat + nu[i + vlen - 2])))
                {
                    --qhat;
                    rhat += nv[vlen - 1];

                    if (rhat > UINT32_MAX)
                        break;
                }

                uint32_t qhat32 = static_cast<uint32_t>(qhat);

                // Multiply and subtract.
                uint32_t carry = MultiplyAndSubstruct(nu.GetData() + i, nv.GetData(), vlen, qhat32);

                int64_t difference = nu[i + vlen] - carry;

                nu[i + vlen] = static_cast<uint32_t>(difference);

                if (difference < 0)
                {
                    --qhat32;
                    carry = common::Add(nu.GetData() + i, nv.GetData(), vlen);

                    assert(carry == 0);
                }

                q[i] = qhat32;
            }

            res.sign = resSign;
            res.Normalize();

            // If remainder is needed unnormolize it.
            if (rem)
            {
                rem->sign = resSign;
                rem->mag.Resize(vlen);

                ShiftRight(nu.GetData(), rem->mag.GetSize(), rem->mag.GetData(), shift);

                rem->Normalize();
            }
        }

        void BigInteger::Normalize()
        {
            int32_t lastNonZero = mag.GetSize() - 1;
            while (lastNonZero >= 0 && mag[lastNonZero] == 0)
                --lastNonZero;

            mag.Resize(lastNonZero + 1);
        }

        bool operator==(const BigInteger& val1, const BigInteger& val2)
        {
            return val1.Compare(val2) == 0;
        }

        bool operator!=(const BigInteger& val1, const BigInteger& val2)
        {
            return val1.Compare(val2) != 0;
        }

        bool operator<(const BigInteger& val1, const BigInteger& val2)
        {
            return val1.Compare(val2) < 0;
        }

        bool operator<=(const BigInteger& val1, const BigInteger& val2)
        {
            return val1.Compare(val2) <= 0;
        }

        bool operator>(const BigInteger& val1, const BigInteger& val2)
        {
            return val1.Compare(val2) > 0;
        }

        bool operator>=(const BigInteger& val1, const BigInteger& val2)
        {
            return val1.Compare(val2) >= 0;
        }
    }
}

